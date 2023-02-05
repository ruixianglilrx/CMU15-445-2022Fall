//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t new_frame_idx;
  if (!free_list_.empty()) {
    new_frame_idx = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&new_frame_idx)) {
    return nullptr;
  }

  *page_id = AllocatePage();
  if (pages_[new_frame_idx].IsDirty()) {
    disk_manager_->WritePage(pages_[new_frame_idx].page_id_, pages_[new_frame_idx].GetData());
  }

  page_table_->Remove(pages_[new_frame_idx].page_id_);
  page_table_->Insert(*page_id, new_frame_idx);
  pages_[new_frame_idx].ResetMemory();
  replacer_->RecordAccess(new_frame_idx);
  replacer_->SetEvictable(new_frame_idx, false);
  pages_[new_frame_idx].page_id_ = *page_id;
  pages_[new_frame_idx].pin_count_ = 1;
  return &pages_[new_frame_idx];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t new_frame_idx;
  if (page_table_->Find(page_id, new_frame_idx)) {
    if (pages_[new_frame_idx].pin_count_ == 0) {
      replacer_->SetEvictable(new_frame_idx, false);
    }
    replacer_->RecordAccess(new_frame_idx);
    pages_[new_frame_idx].pin_count_++;
    return &pages_[new_frame_idx];
  }
  if (GetNewFrame(new_frame_idx)) {
    if (pages_[new_frame_idx].IsDirty()) {
      disk_manager_->WritePage(pages_[new_frame_idx].page_id_, pages_[new_frame_idx].GetData());
    }

    if (pages_[new_frame_idx].pin_count_ == 0) {
      replacer_->RecordAccess(new_frame_idx);
      replacer_->SetEvictable(new_frame_idx, false);
    }
    replacer_->RecordAccess(new_frame_idx);
    page_table_->Remove(pages_[new_frame_idx].page_id_);
    page_table_->Insert(page_id, new_frame_idx);
    pages_[new_frame_idx].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[new_frame_idx].GetData());
    pages_[new_frame_idx].page_id_ = page_id;
    pages_[new_frame_idx].pin_count_++;
    return &pages_[new_frame_idx];
  }

  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_idx;
  if (!page_table_->Find(page_id, frame_idx) || pages_[frame_idx].pin_count_ == 0) {
    return false;
  }

  pages_[frame_idx].pin_count_--;
  if (pages_[frame_idx].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_idx, true);
  }
  pages_[frame_idx].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_idx;
  if (page_table_->Find(page_id, frame_idx)) {
    disk_manager_->WritePage(page_id, pages_[frame_idx].GetData());
    pages_[frame_idx].is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t idx = 0; idx < pool_size_; idx++) {
    FlushPage(pages_[idx].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_idx;
  if (page_table_->Find(page_id, frame_idx)) {
    if (pages_[frame_idx].GetPinCount() != 0) {
      return false;
    }

    page_table_->Remove(page_id);
    replacer_->Remove(frame_idx);
    pages_[frame_idx].ResetMemory();
    free_list_.push_back(frame_idx);
    DeallocatePage(page_id);
    return true;
  }

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetNewFrame(frame_id_t &new_frame_idx) -> bool {
  if (!free_list_.empty()) {
    new_frame_idx = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  return replacer_->Evict(&new_frame_idx);
}
}  // namespace bustub
