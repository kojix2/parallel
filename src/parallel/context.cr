module Parallel
  # Global ExecutionContext for parallel processing
  # Reusing a single context is recommended for performance
  PARALLEL_CONTEXT = Fiber::ExecutionContext::Parallel.new(
    "parallel-workers",
    Fiber::ExecutionContext.default_workers_count
  )
end
