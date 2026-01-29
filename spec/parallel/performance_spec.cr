require "../spec_helper"

if ENV["RUN_PERF_SPECS"]?
  describe "Parallel performance" do
    it "is faster than sequential I/O operations" do
      with_dedicated_execution_context(4) do
        sequential_start = Time.instant
        [1, 2, 3, 4].each do |_|
          sleep(0.02.seconds)
        end
        sequential_elapsed = Time.instant - sequential_start

        parallel_start = Time.instant
        [1, 2, 3, 4].par_each do |_|
          sleep(0.02.seconds)
        end
        parallel_elapsed = Time.instant - parallel_start

        parallel_elapsed.should be < sequential_elapsed
      end
    end
  end
else
  describe "Parallel performance" do
    pending "is skipped unless RUN_PERF_SPECS is set" do
      # Set RUN_PERF_SPECS=1 to run performance specs
    end
  end
end
