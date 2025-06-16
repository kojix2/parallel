# Parallel processing library for Crystal using Fiber::ExecutionContext
#
# This library extends Enumerable and Indexable with parallel processing methods.
# It requires Crystal 1.6.0+ and the following flags:
# -Dpreview_mt -Dexecution_context
#
# Example:
# ```
# [1, 2, 3, 4].par_map { |x| x * 2 } # => [2, 4, 6, 8]
# ```
module Parallel
  VERSION = {{ `shards version #{__DIR__}`.chomp.stringify }}

  # Global ExecutionContext for parallel processing
  # Reusing a single context is recommended for performance
  PARALLEL_CONTEXT = Fiber::ExecutionContext::MultiThreaded.new(
    "parallel-workers",
    Fiber::ExecutionContext.default_workers_count
  )
end

# Extension for Enumerable types (Array, Hash, Set, Range, etc.)
module Enumerable(T)
  # Parallel map operation
  # Applies the given block to each element in parallel and returns an array of results
  #
  # ```
  # [1, 2, 3, 4].par_map { |x| x * 2 } # => [2, 4, 6, 8]
  # ```
  def par_map(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    results = Channel(Tuple(Int32, U) | Exception).new

    items.each_with_index do |item, idx|
      context.spawn do
        begin
          result = block.call(item)
          results.send({idx, result})
        rescue ex
          results.send(ex)
        end
      end
    end

    temp_results = Array(Tuple(Int32, U)).new
    items.size.times do
      case result = results.receive
      when Tuple(Int32, U)
        temp_results << result
      when Exception
        raise result
      end
    end

    # Sort by index and extract values
    temp_results.sort_by(&.[0]).map(&.[1])
  end

  # Parallel each operation
  # Applies the given block to each element in parallel (no return value)
  #
  # ```
  # [1, 2, 3].par_each { |x| puts x }
  # ```
  def par_each(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT
    items = self.to_a
    errors = Channel(Exception).new
    completed = Channel(Nil).new

    items.each do |item|
      context.spawn do
        begin
          block.call(item)
          completed.send(nil)
        rescue ex
          errors.send(ex)
        end
      end
    end

    items.size.times do
      select
      when error = errors.receive
        raise error
      when completed.receive
        # Continue
      end
    end
  end
end

# Optimized extension for Indexable types (Array, Slice, etc.)
module Indexable(T)
  # Optimized parallel map for indexable collections
  # Uses unsafe_fetch for better performance and guarantees order preservation
  def par_map(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, &block : T -> U) forall U
    context = execution_context || Parallel::PARALLEL_CONTEXT
    results = Channel(Tuple(Int32, U) | Exception).new
    temp_results = Array(Tuple(Int32, U)).new

    (0...size).each do |index|
      context.spawn do
        begin
          result = block.call(unsafe_fetch(index))
          results.send({index, result})
        rescue ex
          results.send(ex)
        end
      end
    end

    size.times do
      case result = results.receive
      when Tuple(Int32, U)
        temp_results << result
      when Exception
        raise result
      end
    end

    # Sort by index and extract values to maintain order
    temp_results.sort_by(&.[0]).map(&.[1])
  end

  # Optimized parallel each for indexable collections
  def par_each(execution_context : Fiber::ExecutionContext::MultiThreaded? = nil, &block : T -> _)
    context = execution_context || Parallel::PARALLEL_CONTEXT
    errors = Channel(Exception).new
    completed = Channel(Nil).new

    (0...size).each do |index|
      context.spawn do
        begin
          block.call(unsafe_fetch(index))
          completed.send(nil)
        rescue ex
          errors.send(ex)
        end
      end
    end

    size.times do
      select
      when error = errors.receive
        raise error
      when completed.receive
        # Continue
      end
    end
  end
end
