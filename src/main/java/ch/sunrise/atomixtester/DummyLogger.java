package ch.sunrise.atomixtester;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class DummyLogger implements ILogger {


    @Override
    public void Log(String message, int severity) {
        System.out.println(getTime() + "|" + message);
    }

    private String getTime()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getDefault());
        String DATE_FORMAT = "dd.MM HH:mm:ss";
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getDefault());
        return sdf.format(cal.getTime());
    }

}
