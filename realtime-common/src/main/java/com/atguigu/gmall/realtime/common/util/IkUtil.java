package com.atguigu.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IkUtil {
    public static List<String> IKsplit(String keywords){
        StringReader stringReader = new StringReader(keywords);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,true);
        ArrayList<String> result = new ArrayList<>();
        try {
            Lexeme next = ikSegmenter.next();
            while (next != null){
                result.add(next.getLexemeText());
                next=ikSegmenter.next();
        }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
