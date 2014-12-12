package fr.infologic.vei.audit.migration;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class Mesure
{
    public static final Predicate<Long> IS_10000 = v -> { return 0 == v % 100_000; };
    ThreadLocal<Long> chrono = new ThreadLocal<>();
    String[] dimensions;
    AtomicLong[] counters;
    AtomicLong time = new AtomicLong();
    AtomicLong time0 = new AtomicLong();
    private String task;
    Mesure(String task, String... dimensions)
    {
        this.task = task;
        this.dimensions = dimensions;
        this.counters = new AtomicLong[dimensions.length];
        for(int i = 0; i < counters.length; i++)
        {
            this.counters[i] = new AtomicLong();
        }
    }
    
    public void arm()
    {
        long now = System.currentTimeMillis();
        chrono.set(now);
        time0.compareAndSet(0, now);
    }
    
    public void count(long... increments)
    {
        for (int i = 0; i < increments.length; i++)
        {
            counters[i].addAndGet(increments[i]);
        }
        time.addAndGet(System.currentTimeMillis() - chrono.get());
    }
    
    @Override
    public String toString()
    {
        long elapsed = time.get();
        long real = System.currentTimeMillis() - time0.get();
        StringBuilder message = new StringBuilder(task).append(": time[cumulative: ").append(elapsed/1000).append(" s, elpased: ").append(real / 1000).append(" s], ");
        for(int i = 0; i < dimensions.length; i++)
        {
            if(i > 0)
            {
                message.append(", ");
            }
            long counter = counters[i].get();
            message.append(String.format("%s[%d, %.2f/s, %.2f/s]", dimensions[i], counter, ((double) counter) * 1000 / elapsed, ((double) counter) * 1000 / real));
        }
        return message.toString();
    }
    
    public void printIf(Predicate<Long> condition)
    {
        if(condition.test(counters[0].get()))
        {
            System.out.println(toString());
        }
    }
    
    public static void main(String[] args)
    {
        System.out.println(String.format("%.2f", 1.234));
    }
}
