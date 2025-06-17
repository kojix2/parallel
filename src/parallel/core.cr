# Core module for parallel processing functionality
module Parallel
  VERSION = {{ `shards version #{__DIR__}`.chomp.stringify }}

  # Global ExecutionContext for parallel processing
  # Reusing a single context is recommended for performance
  PARALLEL_CONTEXT = Fiber::ExecutionContext::MultiThreaded.new(
    "parallel-workers",
    Fiber::ExecutionContext.default_workers_count
  )

  # Determines optimal chunk size for adaptive chunking
  def self.adaptive_chunk_size(collection_size : Int32) : Int32
    return 1 if collection_size == 0

    cpu_count = Fiber::ExecutionContext.default_workers_count
    # Use adaptive chunking: aim for cpu_count * 2 chunks, but with reasonable limits
    auto_chunk_size = [1, (collection_size.to_f / (cpu_count * 2)).ceil.to_i].max
    # Clamp to reasonable bounds: min 1, max 1000 to avoid too fine or too coarse granularity
    auto_chunk_size.clamp(1, 1000)
  end

  # Handles parallel each operation with error collection (new behavior - collect all errors)
  def self.handle_each_errors_safe(errors_channel, completed_channel, expected_count : Int32) : Array(Exception)
    collected_errors = [] of Exception
    expected_count.times do
      select
      when error = errors_channel.receive
        collected_errors << error
      when completed_channel.receive
        # Continue
      end
    end
    collected_errors
  end

  # Logs fiber exceptions with context information
  def self.log_fiber_exception(ex : Exception, task_info : String? = nil)
    if task_info
      Crystal.print_buffered("Unhandled exception in parallel task (%s): %s", task_info, ex.message, exception: ex, to: STDERR)
    else
      Crystal.print_buffered("Unhandled exception in parallel task: %s", ex.message, exception: ex, to: STDERR)
    end
  end

  # Ensures cleanup is performed even if exceptions occur
  def self.ensure_cleanup(&cleanup_block)
    at_exit do |exit_code, exception|
      cleanup_block.call
    end
  end
end
