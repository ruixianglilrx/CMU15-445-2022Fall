//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <climits>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  size_t max_distance = 0;
  size_t min_timestamp = LONG_MAX;
  size_t min_record_nums = LONG_MAX;
  frame_id_t result;
  bool flag = false;
  for (auto &it : slots_) {
    if (it.second.record_nums_ == 0 || !it.second.evictable_) {
      continue;
    }

    if (it.second.record_nums_ < k_) {
      if (it.second.record_nums_ < min_record_nums) {
        flag = true;
        result = it.first;
        min_record_nums = it.second.record_nums_;
        min_timestamp = it.second.records_->front();
      } else {
        if (it.second.record_nums_ == min_record_nums && it.second.records_->front() < min_timestamp) {
          flag = true;
          result = it.first;
          min_timestamp = it.second.records_->front();
        }
      }
    } else {
      if (min_record_nums < k_) {
        continue;
      }
      size_t tmp = it.second.records_->front() - it.second.records_->back();
      if (tmp > max_distance) {
        flag = true;
        min_record_nums = k_;
        max_distance = tmp;
        result = it.first;
      }
    }
  }
  if (flag) {
    slots_.erase(result);
    --curr_size_;
    *frame_id = result;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (slots_.find(frame_id) == slots_.end()) {
    BUSTUB_ASSERT((curr_size_ < replacer_size_), "replacer out of space!");
    slots_.emplace(frame_id, Slot());
    slots_[frame_id].records_->push_front(current_timestamp_);
    slots_[frame_id].record_nums_++;
    curr_size_++;
  } else {
    if (slots_[frame_id].record_nums_ == k_) {
      slots_[frame_id].records_->push_front(current_timestamp_);
      slots_[frame_id].records_->pop_back();
    } else {
      slots_[frame_id].records_->push_front(current_timestamp_);
      slots_[frame_id].record_nums_++;
    }
  }
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT((slots_.find(frame_id) != slots_.end()), "frame_id in valid");

  auto &tslot = slots_[frame_id];
  if (tslot.evictable_ && !set_evictable) {
    curr_size_--;
  }
  if (!tslot.evictable_ && set_evictable) {
    curr_size_++;
  }
  tslot.evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (slots_.find(frame_id) == slots_.end()) {
    return;
  }
  BUSTUB_ASSERT((slots_[frame_id].evictable_), "remove NOT_evictable frame");
  curr_size_--;
  slots_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
