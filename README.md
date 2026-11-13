# SlideBolt Scheduler

`sb-scheduler` owns durable timer entities. A timer publishes ordinary switch
on/off commands at one-shot or cron edges. Timers are deliberately level based:
overlapping timers do not reserve their target and missed edges are skipped.
