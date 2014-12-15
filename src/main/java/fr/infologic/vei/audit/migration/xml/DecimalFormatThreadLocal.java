package fr.infologic.vei.audit.migration.xml;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParsePosition;

class DecimalFormatThreadLocal extends ThreadLocal<DecimalFormat>
{
    static Number parse(String string)
    {
        char initialChar = string.charAt(0);
        if((initialChar < '0' || initialChar > '9') && initialChar != '-')
        {
            return null;
        }
        if(initialChar == '0' && string.charAt(1) != '.')
        {
            return null;
        }
        ParsePosition position = new ParsePosition(0);
        Number number = numbers.get().parse(string, position);
        if(position.getErrorIndex() != -1)
        {
            return null;
        }
        if(position.getIndex() != string.length())
        {
            return null;
        }
        if(string.length() > 7 && string.charAt(string.length() - 7) == '.')
        {
            return number.doubleValue();
        }
        long value = number.longValue();
        if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE)
        {
            return number.intValue();
        }
        else
        {
            return number;
        }
    }
    
    private static DecimalFormatThreadLocal numbers = new DecimalFormatThreadLocal();
    
    @Override
    protected DecimalFormat initialValue() 
    {
        DecimalFormat format = new DecimalFormat();
        format.setMinimumIntegerDigits(1);
        DecimalFormatSymbols dfs = new DecimalFormatSymbols();
        dfs.setDecimalSeparator('.');
        dfs.setGroupingSeparator(' ');
        format.setDecimalFormatSymbols(dfs);
        return format;
    }
}
