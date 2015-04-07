require "coolio"

$times = 100000

def add_timeout_timer(loop)
  timeout_timer = Coolio::TimerWatcher.new(60)
  timeout_timer.on_timer do
    timeout_timer.detach
    timeout_timer = nil
  end
  loop.attach(timeout_timer)
  timeout_timer
end

puts "start."

$start = Time.now
$timers = []
$loop = Cool.io::Loop.default
$times.times do
  $timers << add_timeout_timer($loop)
end

$timeout_before = Time.now
timeout_timer = Coolio::TimerWatcher.new(1)
timeout_timer.on_timer do
  $timeout_after = Time.now

  timeout_timer.detach
  $timers.each do |timer|
    timer.detach
  end

  $finish = Time.now
  delta = $finish - $start
  delta -= $timeout_after - $timeout_before
  puts "done."
  puts "overhead: #{delta} seconds for #{$times} timers."
end
$loop.attach(timeout_timer)

$loop.run


