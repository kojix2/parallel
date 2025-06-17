require "./core"

# Extension for Enumerable types (Array, Hash, Set, Range, etc.)
module Enumerable(T)
  # Parallel map operation
  # Applies the given block to each element in parallel and returns an array of results
  # Uses robust error handling internally but maintains fail-fast behavior
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 }           # => [2, 4, 6, 8]
  # [1, 2, 3, 4].par_map(chunk: 2) { |x| x * 2 } # => [2, 4, 6, 8] (same result, fewer context switches)
  # ```
  def par_map(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    return items.map(&block) if items.empty?

    chunk_size = chunk || Parallel.adaptive_chunk_size(items.size)

    Parallel.parallel_map(items.size, context, chunk_size) do |index|
      block.call(items[index])
    end
  end

  # Parallel each operation
  # Applies the given block to each element in parallel (no return value)
  # Uses robust error handling internally but maintains fail-fast behavior
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # [1, 2, 3, 4].par_each(chunk: 2) { |x| puts x } # same result, fewer context switches
  # ```
  def par_each(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, *, chunk : Int32? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    return if items.empty?

    chunk_size = chunk || Parallel.adaptive_chunk_size(items.size)

    Parallel.parallel_each(items.size, context, chunk_size) do |index|
      block.call(items[index])
    end
  end
end
