require "../spec_helper"

describe "Parallel performance" do
  it "handles concurrent access correctly" do
    counter = Atomic(Int32).new(0)

    (1..100).par_each do |_|
      counter.add(1)
    end

    counter.get.should eq(100)
  end

  it "works with I/O operations" do
    start_time = Time.instant

    [1, 2, 3, 4].par_each do |_|
      sleep(0.01.seconds)
    end

    elapsed = Time.instant - start_time
    elapsed.should be < 100.milliseconds
  end
end
