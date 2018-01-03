// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include <type_traits>
#include <utility>

#include <cxxabi.h>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <typeinfo>
#include <unordered_map>

#include <folly/Synchronized.h>

namespace yarpl {

template <typename T>
using Reference = std::shared_ptr<T>;

template <typename T>
struct AtomicReference {
  folly::Synchronized<Reference<T>, std::mutex> ref;

  AtomicReference() = default;

  AtomicReference(Reference<T>&& r) {
    *(ref.lock()) = std::move(r);
  }
};

template <typename T>
Reference<T> atomic_load(AtomicReference<T>* ar) {
  return *(ar->ref.lock());
}

template <typename T>
Reference<T> atomic_exchange(AtomicReference<T>* ar, Reference<T> r) {
  auto refptr = ar->ref.lock();
  auto old = std::move(*refptr);
  *refptr = std::move(r);
  return std::move(old);
}

template <typename T>
void atomic_store(AtomicReference<T>* ar, Reference<T> r) {
  *ar->ref.lock() = std::move(r);
}

class Refcounted {
public:
  virtual ~Refcounted() = default;
};

class enable_get_ref : public std::enable_shared_from_this<enable_get_ref> {
 private:
  virtual void dummy_internal_get_ref() {}

 protected:
  // materialize a reference to 'this', but a type even further derived from
  // Derived, because C++ doesn't have covariant return types on methods
  template <typename As>
  Reference<As> ref_from_this(As* ptr) {
    // at runtime, ensure that the most derived class can indeed be
    // converted into an 'as'
    (void) ptr; // silence 'unused parameter' errors in Release builds
    return std::static_pointer_cast<As>(this->shared_from_this());
  }

  template <typename As>
  Reference<As> ref_from_this(As const* ptr) const {
    // at runtime, ensure that the most derived class can indeed be
    // converted into an 'as'
    (void) ptr; // silence 'unused parameter' errors in Release builds
    return std::static_pointer_cast<As const>(this->shared_from_this());
  }
public:
  virtual ~enable_get_ref() = default;
};

template <typename T, typename CastTo = T, typename... Args>
Reference<CastTo> make_ref(Args&&... args) {
  static_assert(
      std::is_base_of<Refcounted, std::decay_t<T>>::value,
      "Reference can only be constructed with a Refcounted object");

  static_assert(
      std::is_base_of<std::decay_t<CastTo>, std::decay_t<T>>::value,
      "Concrete type must be a subclass of casted-to-type");

  auto r = std::static_pointer_cast<CastTo>(
      std::make_shared<T>(std::forward<Args>(args)...));
  return std::move(r);
}

} /* namespace yarpl */
