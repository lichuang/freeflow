// Copyright (c) 2025 Lichuang(codedump)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::mem::MaybeUninit;
use std::ptr;

use haphazard::AtomicPtr;
use haphazard::HazardPointer;
use haphazard::raw::Pointer;

struct Node<T> {
  data: MaybeUninit<T>,
  next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
  fn new(data: T) -> Self {
    Node {
      data: MaybeUninit::new(data),
      next: unsafe { AtomicPtr::new(ptr::null_mut()) },
    }
  }

  fn sentinel() -> Self {
    Node {
      data: MaybeUninit::uninit(),
      next: unsafe { AtomicPtr::new(ptr::null_mut()) },
    }
  }
}

pub struct Queue<T> {
  head: AtomicPtr<Node<T>>,
  tail: AtomicPtr<Node<T>>,
}

impl<T: Sync + Send> Queue<T> {
  pub fn new() -> Self {
    let sentinel_ptr = Box::new(Node::sentinel()).into_raw();
    Queue {
      head: unsafe { AtomicPtr::new(sentinel_ptr) },
      tail: unsafe { AtomicPtr::new(sentinel_ptr) },
    }
  }

  pub fn push(&self, data: T) {
    let new_node: *mut Node<T> = Box::new(Node::new(data)).into_raw();
    let mut hp = HazardPointer::default();
    // Repeat until completing step 1
    loop {
      // Atomic reads
      let tail = self.tail.safe_load(&mut hp).unwrap();
      let next = tail.next.load_ptr();
      if !next.is_null() {
        // case 1: if next is not null, it means another thread:
        // 1. has change next to a new node
        // 2. but has not update tail
        // Try to help update tail to the (not null) next
        unsafe {
          let _ = self
            .tail
            .compare_exchange_ptr(tail as *const Node<T> as *mut Node<T>, next);
        };
        continue;
      }

      // case 2: next is null
      // Step 1: change next to a new node
      if unsafe {
        tail
          .next
          .compare_exchange_ptr(std::ptr::null_mut(), new_node)
      }
      .is_ok()
      {
        // Step 2: Try to update self.tail to new node
        unsafe {
          let _ = self
            .tail
            .compare_exchange_ptr(tail as *const Node<T> as *mut Node<T>, new_node);
        };
        return;
      }
    }
  }

  pub fn pop(&self) -> Option<T> {
    loop {
      let mut hp_head = HazardPointer::default();
      let mut hp_next = HazardPointer::default();
      // Atomic reads
      let head = self
        .head
        .safe_load(&mut hp_head)
        .expect("queue should never be empty");
      let head_ptr = head as *const Node<T> as *mut Node<T>;
      let next_ptr = head.next.load_ptr();
      let tail_ptr = self.tail.load_ptr();

      // Empty queue
      if head_ptr == tail_ptr {
        return None;
      }

      // Queue is not empty!
      // Dequeue step 1
      let next = head.next.safe_load(&mut hp_next).unwrap();
      if let Ok(unlinked_head_ptr) = unsafe { self.head.compare_exchange_ptr(head_ptr, next_ptr) } {
        // successful dequeue head node from queue
        unsafe {
          unlinked_head_ptr.unwrap().retire();
        }

        // take and return ownership of the data.
        return Some(unsafe { std::ptr::read(next.data.assume_init_ref() as *const _) });
      } else if !next_ptr.is_null() {
        // Help partial enqueue
        // Enqueue step 2
        unsafe {
          let _ = self
            .tail
            .compare_exchange_ptr(tail_ptr as *mut Node<T>, next_ptr);
        }
      }
    }
  }
}

#[cfg(test)]
mod test {
  use std::sync::Arc;
  use std::sync::atomic::AtomicUsize;
  use std::sync::atomic::Ordering;
  use std::thread;

  use super::*;

  #[test]
  fn on_empty_first_pop_is_none() {
    let queue = Queue::<usize>::new();
    assert!(queue.pop().is_none());
  }

  #[test]
  fn on_empty_last_pop_is_none() {
    let queue = Queue::new();
    queue.push(3);
    queue.push(1234);
    queue.pop();
    queue.pop();
    assert!(queue.pop().is_none());
  }

  #[test]
  fn order() {
    let queue = Queue::new();
    queue.push(3);
    queue.push(5);
    queue.push(6);
    assert_eq!(queue.pop(), Some(3));
    assert_eq!(queue.pop(), Some(5));
    assert_eq!(queue.pop(), Some(6));
  }

  #[test]
  fn no_data_corruption() {
    const NTHREAD: usize = 20;
    const NITER: usize = 800;
    const NMOD: usize = 55;

    let queue = Arc::new(Queue::new());
    let mut handles = Vec::with_capacity(NTHREAD);
    let removed = Arc::new(AtomicUsize::new(0));

    for i in 0..NTHREAD {
      let removed = removed.clone();
      let queue = queue.clone();
      handles.push(thread::spawn(move || {
        for j in 0..NITER {
          let val = (i * NITER) + j;
          queue.push(val);
          if (val + 1) % NMOD == 0 {
            if let Some(val) = queue.pop() {
              removed.fetch_add(1, Ordering::Relaxed);
              assert!(val < NITER * NTHREAD);
            }
          }
        }
      }));
    }

    for handle in handles {
      handle.join().expect("thread failed");
    }

    let expected = NITER * NTHREAD - removed.load(Ordering::Relaxed);
    let mut res = 0;
    while let Some(val) = queue.pop() {
      assert!(val < NITER * NTHREAD);
      res += 1;
    }

    assert_eq!(res, expected);
  }
}
