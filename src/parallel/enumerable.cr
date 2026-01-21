require "./core"

# Extension for Enumerable types (Array, Hash, Set, Range, etc.)
module Enumerable(T)
  # Parallel map operation
  # Applies the given block to each element in parallel and returns an array of results
  # Uses lazy evaluation to avoid creating intermediate arrays for memory efficiency
  # Uses robust error handling internally and propagates exceptions
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 }           # => [2, 4, 6, 8]
  # [1, 2, 3, 4].par_map(chunk: 2) { |x| x * 2 } # => [2, 4, 6, 8] (same result, fewer context switches)
  # ```
  def par_map(execution_context : Fiber::ExecutionContext? = nil, *, chunk : Int32? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT

    # Unified empty check
    is_empty, estimated_size = Parallel.check_empty_and_size(self)
    return [] of U if is_empty

    # Validate and normalize chunk size
    chunk_size = Parallel.validate_chunk_size(chunk, estimated_size)

    Parallel.parallel_map_enumerable(self, context, chunk_size, &block)
  end

  # Parallel each operation
  # Applies the given block to each element in parallel (no return value)
  # Uses robust error handling internally and propagates exceptions
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # [1, 2, 3, 4].par_each(chunk: 2) { |x| puts x } # same result, fewer context switches
  # ```
  def par_each(execution_context : Fiber::ExecutionContext? = nil, *, chunk : Int32? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT

    # Unified empty check
    is_empty, estimated_size = Parallel.check_empty_and_size(self)
    return if is_empty

    # Validate and normalize chunk size
    chunk_size = Parallel.validate_chunk_size(chunk, estimated_size)

    # Use lazy evaluation to avoid materializing the entire collection
    Parallel.parallel_each_enumerable(self, context, chunk_size, &block)
  end
end
