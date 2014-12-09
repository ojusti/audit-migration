package fr.infologic.vei.audit.migration.xml;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

class SimpleDateFormatThreadLocal extends ThreadLocal<SimpleDateFormat>
{
    static Date parse(String string) throws ParseException
    {
        try
        {
            return timestamps.get().parse(string);
        }
        catch (ParseException stringIsNotATimestamp)
        {
            return dates.get().parse(string);
        }
    }
    
    private static SimpleDateFormatThreadLocal dates = new SimpleDateFormatThreadLocal("dd/MM/yy");
    private static SimpleDateFormatThreadLocal timestamps = new SimpleDateFormatThreadLocal("dd/MM/yy HH:mm");
    
    private String format;
    private SimpleDateFormatThreadLocal(String format)
    {
        this.format = format;
    }
    
    @Override
    protected SimpleDateFormat initialValue() 
    {
        return getSimpleDateFormat(format);
    }
    
    private static SimpleDateFormat getSimpleDateFormat(String format)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setMinimalDaysInFirstWeek(4);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        simpleDateFormat.setCalendar(calendar);
        return simpleDateFormat;
    }
}
