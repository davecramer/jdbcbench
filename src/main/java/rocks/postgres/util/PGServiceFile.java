package rocks.postgres.util;

import java.text.ParseException;
import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/*
 * See https://www.postgresql.org/docs/current/static/libpq-pgservice.html.
 */
public class PGServiceFile {
    public static PGServiceFile load() {
        PGServiceFile file = new PGServiceFile();
        try {
            String filename = findPath();
            Scanner in = new Scanner(new FileInputStream(filename), "UTF-8");
            try {
                file.parse(in);
            } finally {
                in.close();
            }
        } catch (Exception ex ) {
            ex.printStackTrace();
        }
        return file;
    }

    public Map<String, Map<String, String>> getSections() {
        return sections;
    }

    private static String findPath() {
        String filename = System.getProperty("org.postgresql.pgservicefile");
        if (filename == null) {
            filename = System.getenv().get("PGSERVICEFILE");
        }
        if (filename == null) {
            filename = System.getProperty("user.home") + File.separator + ".pg_service.conf";
        }
        return filename;
    }

    private final Map<String, Map<String, String>> sections;

    public PGServiceFile() {
        sections = new HashMap();
    }

    private void parse(Scanner in) throws Exception {
        int lineNum = 0;
        String sectionName = null;
        Map<String, String> section = null;

        while (in.hasNextLine()) {
            String line = in.nextLine();
            lineNum++;
            line = line.replaceAll("^\\s+", "");
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            } else if (line.startsWith("[")) {
                if (!line.endsWith("]")) {
                    String msg = MessageFormat.format("Error in service file line {0}: missing ].", lineNum);
                    throw new ParseException(msg, lineNum);
                }
                sectionName = line.substring(1, line.length() - 1);
                section = new HashMap();
                sections.put(sectionName, section);
            } else if (section == null) {
                String msg = MessageFormat.format("Error in service file line {0}: not in section.", lineNum);
                throw new ParseException(msg, lineNum);
            } else {
                String[] segment = line.split("=", 2);
                if (segment.length != 2) {
                    String msg = MessageFormat.format("Error in service file line {0}: bad syntax.", lineNum);
                    throw new ParseException(msg, lineNum);
                }
                section.put(segment[0], segment[1]);
            }
        }
    }

    public Map<String, String> getService(String name) throws SQLException {
        if (sections.containsKey(name)) {
            return sections.get(name);
        } else {
            throw new SQLException(MessageFormat.format("Unknown service {0}.", name));
        }
    }

    public static void copyProperties(Map<String, String> service, Properties urlProps) throws Exception {
        if (service.containsKey("port")) {
            urlProps.setProperty("PGPORT", service.get("port"));
        }
        if (service.containsKey("host")) {
            urlProps.setProperty("PGHOST", service.get("host"));
        }
        if (service.containsKey("dbname")) {
            urlProps.setProperty("PGDBNAME", service.get("dbname"));
        }
        for (Map.Entry<String, String> entry : service.entrySet()) {
            urlProps.setProperty(entry.getKey(), entry.getValue());
        }
    }
}

