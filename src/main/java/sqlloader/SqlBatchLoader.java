package sqlloader;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL 檔批次匯入 Oracle 工具
 * <p>
 * 功能重點：
 * 1) 以串流方式讀取超大檔 (1GB+)，按 ';' 分割語句（避開字串/註解/括號內的分號）。
 * 2) 僅處理 INSERT 語句；透過 JDBC batch 提升效能。
 * 3) 可選擇忽略 PK 欄位 WCSID（會從欄位清單與對應值移除）。
 * 4) 顯示讀檔/寫入進度百分比。
 * 5) 正確處理字串內特殊字元（單引號、雙引號、反斜線）與註解。
 * 6) Windows 無網路環境；不依賴第三方函式庫。
 * <p>
 * 參數：
 * --jdbcUrl       (必填) 例如 jdbc:oracle:thin:@//host:1521/serviceName
 * --user          (必填) 資料庫帳號
 * --password      (選填) 不提供則會互動式輸入
 * --file          (必填) SQL 檔路徑
 * --batchSize     (選填) 預設 500
 * --ignorePK      (選填) true/false 預設 false。true 時會移除 WCSID 欄位與值
 * --pkName        (選填) 預設 WCSID（不分大小寫比對）
 * --charset       (選填) 檔案編碼，預設 UTF-8
 * --continueOnError (選填) true/false 預設 false。true 表示錯誤時記錄並繼續
 * <p>
 * 範例：
 * java -cp "sql-batch-loader.jar;ojdbc8.jar" com.example.main.java.sqlloader.SqlBatchLoader ^
 * --jdbcUrl "jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1" ^
 * --user AP_TAX ^
 * --password 123456 ^
 * --file "D:\data\wcst172.sql" ^
 * --batchSize 1000 ^
 * --ignorePK true
 * <p>
 *
 * @author Cheng
 * @since 2025-08-28 15:22
 */
public class SqlBatchLoader {

    private static class Config {
        String jdbcUrl;
        String user;
        String password;
        Path sqlFile;
        int batchSize = 500;
        boolean ignorePk = false;
        String pkName = "WCSID";
        Charset charset = StandardCharsets.UTF_8;
        boolean continueOnError = false;
    }

    public static void main(String[] args) {
        Config cfg = parseArgs(args);
        if (cfg == null) {
            printUsage();
            System.exit(2);
        }

        if (cfg.password == null) {
            cfg.password = promptPassword("請輸入資料庫密碼: ");
        }

        if (!Files.isReadable(cfg.sqlFile)) {
            System.err.println("[錯誤] 找不到或不可讀取檔案: " + cfg.sqlFile);
            System.exit(3);
        }

        try {
            run(cfg);
        } catch (Exception e) {
            System.err.println("\n[致命錯誤] " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void run(Config cfg) throws Exception {
        long totalBytes = Files.size(cfg.sqlFile);
        System.out.printf("開始預掃描（僅計算語句數）... 檔案大小：%,d bytes%n", totalBytes);

        long totalStatements = countStatements(cfg.sqlFile, cfg.charset, totalBytes);

        System.out.printf("預掃描完成，偵測到 INSERT 語句總數：約 %,d 條%n", totalStatements);

        Instant start = Instant.now();
        Properties props = new Properties();
        props.put("user", cfg.user);
        props.put("password", cfg.password);

        try (Connection conn = DriverManager.getConnection(cfg.jdbcUrl, props)) {
            conn.setAutoCommit(false);

            try (Statement ping = conn.createStatement()) {
                ping.execute("SELECT 1 FROM DUAL");
            }

            System.out.println("開始匯入 ...");
            final AtomicLong executed = new AtomicLong();
            final AtomicLong failed = new AtomicLong();
            final AtomicLong batched = new AtomicLong();
            final Instant[] lastLog = {Instant.now()};

            try (InputStream raw = Files.newInputStream(cfg.sqlFile);
                 CountingInputStream cin = new CountingInputStream(raw);
                 Reader reader = new BufferedReader(new InputStreamReader(cin, cfg.charset), 1 << 16);
                 Statement stmt = conn.createStatement()) {

                StatementSplitter splitter = new StatementSplitter();
                char[] buf = new char[1 << 15]; // 32KB
                int len;
                while ((len = reader.read(buf)) != -1) {
                    splitter.accept(buf, 0, len, sql -> {
                        String trimmed = sql.trim();
                        if (trimmed.isEmpty()) return;
                        if (!startsWithIgnoreCase(trimmed, "INSERT")) return;

                        String toExec = trimmed;
                        if (cfg.ignorePk) {
                            try {
                                toExec = SqlInsertRewriter.removeColumnFromInsert(trimmed, cfg.pkName);
                            } catch (RuntimeException ex) {
                                if (cfg.continueOnError) {
                                    System.err.println("\n[警告] 無法移除 PK，語句略過並計入失敗： " + ex.getMessage());
                                    failed.getAndIncrement();
                                    return;
                                } else {
                                    throw ex;
                                }
                            }
                        }

                        // Debug: 檢查是否包含長字串
                        boolean hasLongString = containsLongStringLiteral(toExec);
                        if (hasLongString) {
                            System.out.println("\n[DEBUG] 偵測到長字串語句，將單獨處理");
                            System.out.println("[DEBUG] SQL 長度: " + toExec.length() + " 字元");
                            // 顯示前 200 個字元
                            String preview = toExec.length() > 200 ? toExec.substring(0, 200) + "..." : toExec;
                            System.out.println("[DEBUG] SQL 預覽: " + preview);
                        }
                        try {
                            if (hasLongString) {
                                // 有長字串 → 先執行現有批次，再單獨處理長字串
                                if (batched.get() > 0) {
                                    try {
                                        int[] rs = stmt.executeBatch();
                                        conn.commit();
                                        executed.addAndGet(successCount(rs));
                                        batched.set(0);
                                    } catch (SQLException ex) {
                                        if (cfg.continueOnError) {
                                            System.err.println("\n[錯誤] 批次執行失敗：" + ex.getMessage());
                                            failed.addAndGet(batched.get());
                                            safeFlush(stmt, conn);
                                            batched.set(0);
                                        } else {
                                            throw new RuntimeException("批次執行失敗：" + ex.getMessage(), ex);
                                        }
                                    }
                                }

                                // 單獨處理長字串語句
                                try (PreparedStatement ps = buildPreparedStatement(conn, toExec)) {
                                    ps.executeUpdate();
                                    conn.commit();
                                    executed.incrementAndGet();
                                } catch (SQLException ex) {
                                    if (cfg.continueOnError) {
                                        System.err.println("\n[錯誤] 長字串語句執行失敗：" + ex.getMessage());
                                        failed.getAndIncrement();
                                    } else {
                                        throw new RuntimeException("長字串語句執行失敗：" + ex.getMessage(), ex);
                                    }
                                }
                            } else {
                                // 沒有長字串 → 走批次
                                stmt.addBatch(toExec);
                                batched.getAndIncrement();
                                if (batched.get() >= cfg.batchSize) {
                                    System.out.println("\n[DEBUG] 執行批次，大小: " + batched.get());
                                    try {
                                        int[] rs = stmt.executeBatch();
                                        conn.commit();
                                        executed.addAndGet(successCount(rs));
                                        batched.set(0);
                                    } catch (SQLException ex) {
                                        System.err.println("\n[DEBUG] 批次執行失敗，錯誤訊息: " + ex.getMessage());
                                        if (cfg.continueOnError) {
                                            failed.addAndGet(batched.get());
                                            safeFlush(stmt, conn);
                                            batched.set(0);
                                        } else {
                                            throw ex;
                                        }
                                    }
                                }
                            }
                        } catch (SQLException ex) {
                            if (cfg.continueOnError) {
                                System.err.println("\n[錯誤] 單筆加入/執行失敗：" + ex.getMessage());
                                safeFlush(stmt, conn);
                                failed.getAndIncrement();
                            } else {
                                throw new RuntimeException("批次執行失敗：" + ex.getMessage(), ex);
                            }
                        }

                        Instant now = Instant.now();
                        if (Duration.between(lastLog[0], now).toMillis() >= 500) {
                            lastLog[0] = now;
                            printProgress(cin.getCount(), totalBytes, executed.get(), totalStatements);
                        }
                    });
                }

                if (batched.get() > 0) {
                    try {
                        int[] rs = stmt.executeBatch();
                        conn.commit();
                        executed.addAndGet(successCount(rs));
                    } catch (SQLException ex) {
                        if (cfg.continueOnError) {
                            System.err.println("\n[錯誤] 最後批次執行失敗：" + ex.getMessage());
                            failed.addAndGet(batched.get());
                            safeFlush(stmt, conn);
                        } else {
                            throw new RuntimeException("最後批次執行失敗：" + ex.getMessage(), ex);
                        }
                    }
                }
            }

            Duration took = Duration.between(start, Instant.now());
            long minutes = took.toMinutes();
            long seconds = took.getSeconds() - minutes * 60;
            System.out.println("\n=== 匯入完成 ===");
            System.out.printf("成功：%,d 條；失敗：%,d 條；總語句：%,d 條%n",
                    executed.get(), failed.get(), totalStatements);
            System.out.printf("耗時：約 %d 分 %d 秒%n", minutes, seconds);
        }
    }

    private static long countStatements(Path file, Charset cs, long totalBytes) throws IOException {
        AtomicLong count = new AtomicLong();
        Instant lastLog = Instant.now();
        try (InputStream raw = Files.newInputStream(file);
             CountingInputStream cin = new CountingInputStream(raw);
             Reader reader = new BufferedReader(new InputStreamReader(cin, cs), 1 << 16)) {

            StatementSplitter splitter = new StatementSplitter();
            char[] buf = new char[1 << 15];
            int len;
            while ((len = reader.read(buf)) != -1) {
                splitter.accept(buf, 0, len, sql -> {
                    String t = sql.trim();
                    if (!t.isEmpty() && startsWithIgnoreCase(t, "INSERT")) {
                        count.getAndIncrement();
                    }
                });
                Instant now = Instant.now();
                if (Duration.between(lastLog, now).toMillis() >= 500) {
                    lastLog = now;
                    printReadProgress(cin.getCount(), totalBytes);
                }
            }
        }
        printReadProgress(totalBytes, totalBytes);
        return count.get();
    }

    private static void printProgress(long readBytes, long totalBytes, long executed, long totalStatements) {
        printReadProgress(readBytes, totalBytes);
        int dbPct = totalStatements > 0 ? (int) Math.min(100, (executed * 100) / totalStatements) : 0;
        System.out.printf(" | 寫入進度：%3d%%（%,d/%,d）\r", dbPct, executed, totalStatements);
    }

    private static void printReadProgress(long readBytes, long totalBytes) {
        int pct = totalBytes > 0 ? (int) Math.min(100, (readBytes * 100) / totalBytes) : 0;
        System.out.printf("讀取進度：%3d%%（%,d/%,d bytes）", pct, readBytes, totalBytes);
    }

    private static int successCount(int[] updateCounts) {
        int ok = 0;
        for (int c : updateCounts) {
            if (c >= 0 || c == Statement.SUCCESS_NO_INFO) ok++;
        }
        return ok;
    }

    private static void safeFlush(Statement stmt, Connection conn) {
        try {
            stmt.clearBatch();
        } catch (SQLException ignore) {
        }
        try {
            conn.rollback();
        } catch (SQLException ignore) {
        }
    }

    private static boolean startsWithIgnoreCase(String s, String prefix) {
        return s.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private static Config parseArgs(String[] args) {
        Config cfg = new Config();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--jdbcUrl":
                    cfg.jdbcUrl = nextArg(args, ++i, "--jdbcUrl");
                    break;
                case "--user":
                    cfg.user = nextArg(args, ++i, "--user");
                    break;
                case "--password":
                    cfg.password = nextArg(args, ++i, "--password");
                    break;
                case "--file":
                    cfg.sqlFile = Paths.get(nextArg(args, ++i, "--file"));
                    break;
                case "--batchSize":
                    cfg.batchSize = Integer.parseInt(nextArg(args, ++i, "--batchSize"));
                    break;
                case "--ignorePK":
                    cfg.ignorePk = Boolean.parseBoolean(nextArg(args, ++i, "--ignorePK"));
                    break;
                case "--pkName":
                    cfg.pkName = nextArg(args, ++i, "--pkName");
                    break;
                case "--charset":
                    cfg.charset = Charset.forName(nextArg(args, ++i, "--charset"));
                    break;
                case "--continueOnError":
                    cfg.continueOnError = Boolean.parseBoolean(nextArg(args, ++i, "--continueOnError"));
                    break;
                default:
                    System.err.println("未知參數: " + a);
                    return null;
            }
        }
        if (cfg.jdbcUrl == null || cfg.user == null || cfg.sqlFile == null) {
            return null;
        }
        return cfg;
    }

    private static String nextArg(String[] args, int idx, String name) {
        if (idx >= args.length) throw new IllegalArgumentException("缺少參數值：" + name);
        return args[idx];
    }

    private static void printUsage() {
        System.out.println("用法：");
        System.out.println("java -cp \"sql-batch-loader.jar;ojdbc8.jar\" sqlloader.SqlBatchLoader \\");
        System.out.println("  --jdbcUrl \"jdbc:oracle:thin:@//host:1521/service\" --user USER --password PASS \\");
        System.out.println("  --file \"D:\\\\path\\\\file.sql\" [--batchSize 1000] [--ignorePK true]");
    }

    private static String promptPassword(String prompt) {
        Console c = System.console();
        if (c != null) {
            char[] pw = c.readPassword(prompt);
            return pw == null ? null : new String(pw);
        } else {
            System.out.print(prompt);
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                return br.readLine();
            } catch (IOException e) {
                return null;
            }
        }
    }

    static class CountingInputStream extends FilterInputStream {
        private long count = 0L;

        CountingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) count++;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) count += n;
            return n;
        }

        long getCount() {
            return count;
        }
    }

    private static boolean containsLongStringLiteral(String sql) {
        int pos = 0;
        while ((pos = sql.indexOf('\'', pos)) != -1) {
            int end = findMatchingQuote(sql, pos);
            if (end > pos) {
                String literal = sql.substring(pos + 1, end);
                int bytes = literal.getBytes(StandardCharsets.UTF_8).length;
                if (bytes > 4000) {  // Oracle 單一字串常值上限
                    System.out.println("[DEBUG] 發現長字串: " + bytes + " bytes, 位置: " + pos + "-" + end);
                    return true;
                }
                pos = end + 1;
            } else {
                break;
            }
        }
        return false;
    }

    private static int findMatchingQuote(String sql, int startQuotePos) {
        if (startQuotePos < 0 || startQuotePos >= sql.length() || sql.charAt(startQuotePos) != '\'') {
            return -1;
        }

        for (int i = startQuotePos + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // 檢查是否為逃脫的單引號 ('')
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++; // 跳過下一個單引號
                } else {
                    return i; // 找到匹配的結束引號
                }
            }
        }
        return -1; // 沒有找到匹配的結束引號
    }

    private static PreparedStatement buildPreparedStatement(Connection conn, String sql) throws SQLException {
        int idxValues = sql.toUpperCase().indexOf("VALUES");
        if (idxValues < 0) throw new SQLException("語法錯誤，找不到 VALUES: " + sql);

        String beforeValues = sql.substring(0, idxValues);
        int openParen = sql.indexOf('(', idxValues);
        int closeParen = SqlInsertRewriter.findMatchingParen(sql, openParen);

        String valuesRegion = sql.substring(openParen + 1, closeParen);
        List<String> values = SqlInsertRewriter.splitTopLevel(valuesRegion);

        // 改成佔位符
        String newSql = beforeValues + "VALUES (" + String.join(", ", Collections.nCopies(values.size(), "?")) + ")";
        PreparedStatement ps = conn.prepareStatement(newSql);

        for (int i = 0; i < values.size(); i++) {
            String v = values.get(i).trim();

            if (v.equalsIgnoreCase("NULL")) {
                ps.setNull(i + 1, Types.NULL);

            } else if (v.toUpperCase().startsWith("DATE '")) {
                String literal = v.substring(5).trim();
                String dateStr = literal.substring(1, literal.length() - 1);
                try {
                    ps.setTimestamp(i + 1, Timestamp.valueOf(dateStr.replace('T', ' ')));
                } catch (IllegalArgumentException e) {
                    ps.setDate(i + 1, Date.valueOf(dateStr));
                }

            } else if (v.toUpperCase().startsWith("TIMESTAMP '")) {
                String literal = v.substring(10).trim();
                String tsStr = literal.substring(1, literal.length() - 1);
                try {
                    ps.setTimestamp(i + 1, Timestamp.valueOf(tsStr.replace('T', ' ')));
                } catch (IllegalArgumentException e) {
                    // Try to parse Chinese month format like "19-8月 -25 10.30.00.000000000"
                    ps.setTimestamp(i + 1, parseChineseMonthTimestamp(tsStr));
                }

            } else if (v.toUpperCase().startsWith("TO_TIMESTAMP(")) {
                // Handle to_timestamp function like "to_timestamp('19-8月 -25 10.30.00.000000000 上午','DD-MON-RR HH.MI.SSXFF AM')"
                ps.setTimestamp(i + 1, parseToTimestampFunction(v));

            } else if (v.startsWith("'") && v.endsWith("'")) {
                String text = v.substring(1, v.length() - 1).replace("''", "'");
                int bytes = text.getBytes(StandardCharsets.UTF_8).length;

                if (bytes > 4000 || text.length() > 2000) {
                    // 超過 Oracle 字串限制 → 改用 CLOB
                    Reader r = new StringReader(text);
                    ps.setCharacterStream(i + 1, r, text.length());
                } else {
                    ps.setString(i + 1, text);
                }

            } else {
                try {
                    if (v.contains(".")) {
                        ps.setDouble(i + 1, Double.parseDouble(v));
                    } else {
                        ps.setLong(i + 1, Long.parseLong(v));
                    }
                } catch (NumberFormatException e) {
                    ps.setString(i + 1, v);
                }
            }
        }

        return ps;
    }

    /**
     * 解析中文月份格式的時間戳，例如 "19-8月 -25 10.30.00.000000000"
     */
    private static Timestamp parseChineseMonthTimestamp(String timestampStr) {
        try {
            // 處理格式如 "19-8月 -25 10.30.00.000000000"
            String normalized = timestampStr.replace("月", "").trim();

            // 使用 DateTimeFormatter 解析格式
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-M-yyyy HH.mm.ss.SSSSSSSSS", Locale.TAIWAN);
            LocalDateTime dateTime = LocalDateTime.parse(normalized, formatter);
            return Timestamp.valueOf(dateTime);
        } catch (DateTimeParseException e) {
            // 嘗試其他可能的格式變體
            try {
                // 處理可能的空格變化
                String cleaned = timestampStr.replaceAll("\\s+", " ").replace("月", "");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-M-yyyy HH.mm.ss.SSSSSSSSS", Locale.TAIWAN);
                LocalDateTime dateTime = LocalDateTime.parse(cleaned, formatter);
                return Timestamp.valueOf(dateTime);
            } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException("無法解析時間戳格式: " + timestampStr, e2);
            }
        }
    }

    /**
     * 解析 to_timestamp 函數，支援完整語法如 "to_timestamp('24-1月-24 10.30.00.000000000 上午','DD-MON-RR HH.MI.SSXFF AM','NLS_DATE_LANGUAGE=TRADITIONAL CHINESE')"
     */
    private static Timestamp parseToTimestampFunction(String functionStr) {
        try {
            // 提取所有參數
            List<String> parameters = extractFunctionParameters(functionStr);

            if (parameters.size() < 2) {
                throw new IllegalArgumentException("to_timestamp 函數參數不足: " + functionStr);
            }

            String timestampStr = parameters.get(0);
            String formatStr = parameters.get(1);

            // 檢查是否有 NLS_DATE_LANGUAGE 參數
            boolean hasChineseNLS = false;
            if (parameters.size() >= 3) {
                String nlsParam = parameters.get(2).toUpperCase();
                hasChineseNLS = nlsParam.contains("TRADITIONAL CHINESE") ||
                        nlsParam.contains("SIMPLIFIED CHINESE") ||
                        nlsParam.contains("CHINESE");
            }

            // 根據格式字符串決定解析方式
            if (formatStr.toUpperCase().contains("MON") &&
                    (timestampStr.contains("月") || hasChineseNLS)) {
                // 處理中文月份格式
                return parseChineseMonthWithAmPm(timestampStr);
            } else {
                // 嘗試標準格式解析
                return parseStandardTimestamp(timestampStr, formatStr);
            }

        } catch (Exception e) {
            throw new IllegalArgumentException("無法解析 to_timestamp 函數: " + functionStr, e);
        }
    }

    /**
     * 提取函數參數列表
     */
    private static List<String> extractFunctionParameters(String functionStr) {
        List<String> parameters = new ArrayList<>();

        // 找到括號內容
        int startParen = functionStr.indexOf('(');
        int endParen = functionStr.lastIndexOf(')');

        if (startParen == -1 || endParen == -1) {
            return parameters;
        }

        String paramStr = functionStr.substring(startParen + 1, endParen).trim();

        // 解析參數（按逗號分隔，但考慮字串內的逗號）
        StringBuilder currentParam = new StringBuilder();
        boolean inString = false;

        for (int i = 0; i < paramStr.length(); i++) {
            char c = paramStr.charAt(i);

            if (c == '\'') {
                inString = !inString;
                currentParam.append(c);
            } else if (c == ',' && !inString) {
                parameters.add(currentParam.toString().trim());
                currentParam.setLength(0);
            } else {
                currentParam.append(c);
            }
        }

        // 添加最後一個參數
        if (currentParam.length() > 0) {
            parameters.add(currentParam.toString().trim());
        }

        return parameters;
    }

    /**
     * 解析標準時間戳格式
     */
    private static Timestamp parseStandardTimestamp(String timestampStr, String formatStr) {
        try {
            // 移除字串引號
            String cleanStr = timestampStr.startsWith("'") && timestampStr.endsWith("'") ?
                    timestampStr.substring(1, timestampStr.length() - 1) : timestampStr;

            // 嘗試常見的標準格式
            if (cleanStr.contains("T")) {
                // ISO 格式
                return Timestamp.valueOf(cleanStr.replace('T', ' '));
            } else {
                // 嘗試其他標準格式
                return Timestamp.valueOf(cleanStr);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("無法解析標準時間戳格式: " + timestampStr, e);
        }
    }

    /**
     * 解析包含中文月份和上午/下午的時間戳格式
     */
    private static Timestamp parseChineseMonthWithAmPm(String timestampStr) {
        try {
            // 處理格式如 "19-8月 -25 10.30.00.000000000 上午"
            String normalized = timestampStr.replace("月", "").trim();

            // 處理上午/下午標示
            boolean isPM = normalized.contains("下午");
            boolean isAM = normalized.contains("上午");

            // 移除上午/下午標示
            String timePart = normalized.replaceAll("(上午|下午)", "").trim();

            // 使用 DateTimeFormatter 解析格式
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-M-yyyy HH.mm.ss.SSSSSSSSS", Locale.TAIWAN);
            LocalDateTime dateTime = LocalDateTime.parse(timePart, formatter);

            // 調整下午時間
            if (isPM && dateTime.getHour() < 12) {
                dateTime = dateTime.plusHours(12);
            }
            // 調整上午12點為0點
            if (isAM && dateTime.getHour() == 12) {
                dateTime = dateTime.minusHours(12);
            }

            return Timestamp.valueOf(dateTime);

        } catch (DateTimeParseException e) {
            // 嘗試其他可能的格式變體
            try {
                String cleaned = timestampStr.replaceAll("\\s+", " ").replace("月", "");
                boolean isPM = cleaned.contains("下午");
                boolean isAM = cleaned.contains("上午");
                String timePart = cleaned.replaceAll("(上午|下午)", "").trim();

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-M-yyyy HH.mm.ss.SSSSSSSSS", Locale.TAIWAN);
                LocalDateTime dateTime = LocalDateTime.parse(timePart, formatter);

                if (isPM && dateTime.getHour() < 12) {
                    dateTime = dateTime.plusHours(12);
                }
                if (isAM && dateTime.getHour() == 12) {
                    dateTime = dateTime.minusHours(12);
                }

                return Timestamp.valueOf(dateTime);
            } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException("無法解析時間戳格式: " + timestampStr, e2);
            }
        }
    }

}