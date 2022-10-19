package org.ergemp.sql.sinks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SaveToHadoop {
    public static void main(String[] args) {
        //suppress logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //df.write.option("path", "[path-here]").saveAsTable("tableName")
    }
}
