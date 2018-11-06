package com.addthis.metrics3.reporter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

public class ConverterTest {

    Converter converter;

    {
        Map<Pattern,UnitConvertion> map = new HashMap<Pattern, UnitConvertion>();
        map.put(Pattern.compile("^.*.75th"), UnitConvertion.BYTES_TO_KB);
        converter = new Converter(map);
    }

    @Test
    public void convertion(){
        Assert.assertEquals(Float.valueOf(2),Float.valueOf(converter.convert("Prueba.75th", 2048)));
    }

    @Test
    public void noConvertion(){
        Assert.assertEquals(Float.valueOf(2048),Float.valueOf(converter.convert("Prueba.90th", 2048)));

    }


}
