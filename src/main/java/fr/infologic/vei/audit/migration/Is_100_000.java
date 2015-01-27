package fr.infologic.vei.audit.migration;

import java.util.function.Predicate;

public class Is_100_000 implements Predicate<Long>
{
    public static final Is_100_000 TESTER = new Is_100_000();

    @Override
    public boolean test(Long v)
    {
        return 0 == v % 100_000;
    }
}
