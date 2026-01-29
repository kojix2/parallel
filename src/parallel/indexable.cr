require "fiber/execution_context"
require "./core"

# Optimized extension for Indexable types (Array, Slice, etc.)
module Indexable(T)
  # Optimized parallel map for indexable collections
  # Uses unsafe_fetch for better performance and guarantees order preservation
  # Uses robust error handling internally and propagates exceptions
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 }           # => [2, 4, 6, 8]
  # [1, 2, 3, 4].par_map(chunk: 2) { |x| x * 2 } # => [2, 4, 6, 8] (same result, fewer context switches)
  # ```
  def par_map(execution_context : Fiber::ExecutionContext::Parallel? = nil, *, chunk : Int32? = nil, &block : T -> U) forall U
    context = execution_context || Parallel.execution_context

    # Unified empty check
    is_empty, collection_size = Parallel.check_empty_and_size(self)

    # Validate and normalize chunk size
    chunk_size = Parallel.validate_chunk_size(chunk, collection_size)
    return [] of U if is_empty

    Parallel.parallel_map_indexable(collection_size, context, chunk_size) do |index|
      block.call(unsafe_fetch(index))
    end
  end

  # Optimized parallel each for indexable collections
  # Uses robust error handling internally and propagates exceptions
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # [1, 2, 3, 4].par_each(chunk: 2) { |x| puts x } # same result, fewer context switches
  # ```
  def par_each(execution_context : Fiber::ExecutionContext::Parallel? = nil, *, chunk : Int32? = nil, &block : T -> _)
    context = execution_context || Parallel.execution_context

    # Unified empty check
    is_empty, collection_size = Parallel.check_empty_and_size(self)

    # Validate and normalize chunk size
    chunk_size = Parallel.validate_chunk_size(chunk, collection_size)
    return if is_empty

    Parallel.parallel_each(collection_size, context, chunk_size) do |index|
      block.call(unsafe_fetch(index))
    end
  end
end
