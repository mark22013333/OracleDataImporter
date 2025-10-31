package sqlloader;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 專門把 INSERT 語句中的某個欄位（例如 WCSID）從欄位清單及對應值中移除。
 * <p>
 * 僅支援形如：
 * INSERT INTO <table> (col1, col2, ...) VALUES (val1, val2, ...)
 * （欄位清單必須存在；若語句無欄位清單，無法判定對應位置，將丟出例外）
 * <p>
 * 會保留值中的特殊語法（NULL、DATE '...', TIMESTAMP '...'、字串、數字等）。
 *
 * @author Cheng
 * @since 2025-08-28 15:24
 **/
public class SqlInsertRewriter {
    // 偵測 DATE 'YYYY-MM-DD HH24:MI:SS...' 的正則
    private static final Pattern DATE_TIME_PATTERN =
            Pattern.compile("DATE '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?'");

    public static String removeColumnFromInsert(String insertSql, String columnName) {
        String sql = insertSql.trim();
        int idxInsert = indexOfIgnoreCase(sql, "INSERT");
        int idxInto = indexOfIgnoreCase(sql, "INTO");
        if (idxInsert != 0 || idxInto < 0) {
            // 非標準 INSERT INTO 開頭，直接返回
            return sql;
        }

        // 找到欄位清單的 '('
        int openCols = nextChar(sql, ')', indexOfOpenParenAfterInto(sql, idxInto));
        if (openCols < 0) {
            // 未找到 '('；代表沒有欄位清單，無法移除
            throw new RuntimeException("語句未提供欄位清單，無法移除欄位：" + columnName);
        }
        int startCols = openCols; // '(' 位置
        int endCols = findMatchingParen(sql, startCols);

        String colsRegion = sql.substring(startCols + 1, endCols);
        List<String> columns = splitTopLevel(colsRegion);

        int removeAt = -1;
        for (int i = 0; i < columns.size(); i++) {
            String c = unquoteIdentifier(columns.get(i).trim());
            if (c.equalsIgnoreCase(columnName)) {
                removeAt = i;
                break;
            }
        }
        if (removeAt < 0) {
            // 沒有該欄位，直接返回原始 SQL
            return sql;
        }

        // 找 VALUES
        int idxValuesKw = indexOfIgnoreCase(sql, "VALUES", endCols);
        if (idxValuesKw < 0) {
            throw new RuntimeException("找不到 VALUES 子句，無法重寫。");
        }
        // 找 values '('
        int openVals = sql.indexOf('(', idxValuesKw);
        if (openVals < 0) throw new RuntimeException("找不到 VALUES 後的 '('。");
        int endVals = findMatchingParen(sql, openVals);

        String valsRegion = sql.substring(openVals + 1, endVals);
        List<String> values = splitTopLevel(valsRegion);

        if (values.size() != columns.size()) {
            throw new RuntimeException("欄位數與值數不一致（" + columns.size() + " vs " + values.size() + "），無法重寫。");
        }

        // 移除
        columns.remove(removeAt);
        values.remove(removeAt);

        // 重建 SQL
        String prefix = sql.substring(0, startCols).trim();
        String afterVals = sql.substring(endVals + 1).trim(); // 若將來有 RETURNING/LOG ERRORS 可保留
        String newSql = prefix + " (" + String.join(", ", columns) + ") VALUES (" + String.join(", ", values) + ")";
        if (!afterVals.isEmpty()) {
            newSql += " " + afterVals;
        }

        // 在這裡處理 DATE → TIMESTAMP
        Matcher matcher = DATE_TIME_PATTERN.matcher(newSql);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, matcher.group().replaceFirst("DATE", "TIMESTAMP"));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * 找到 INTO 後第一個 '(' 的位置
     */
    private static int indexOfOpenParenAfterInto(String s, int idxInto) {
        int i = idxInto + 4; // "INTO".length()
        // 跳過空白與表名/模式名/雙引號識別字，直到 '('
        boolean inDoubleQuote = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                // toggle double quote
                if (inDoubleQuote) {
                    // look for escaped ""
                    if (i + 1 < s.length() && s.charAt(i + 1) == '"') {
                        i++;
                        continue;
                    } else {
                        inDoubleQuote = false;
                    }
                } else {
                    inDoubleQuote = true;
                }
                continue;
            }
            if (inDoubleQuote) continue;
            if (c == '(') return i;
        }
        return -1;
    }

    /**
     * 在 startPos 後找 target 字串（不分大小寫）
     */
    private static int indexOfIgnoreCase(String s, String target, int startPos) {
        String lower = s.toLowerCase();
        String t = target.toLowerCase();
        return lower.indexOf(t, startPos);
    }

    /**
     * 從 0 開頭找（不分大小寫）
     */
    private static int indexOfIgnoreCase(String s, String target) {
        return indexOfIgnoreCase(s, target, 0);
    }

    /**
     * 回傳在 pos 的 '(' 對應的 ')' 位置，考慮字串/雙引號與嵌套
     */
    protected static int findMatchingParen(String s, int posOpenParen) {
        if (posOpenParen < 0 || posOpenParen >= s.length() || s.charAt(posOpenParen) != '(')
            throw new IllegalArgumentException("posOpenParen 非 '(' 位置");

        int depth = 0;
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = posOpenParen; i < s.length(); i++) {
            char c = s.charAt(i);
            char next = (i + 1 < s.length()) ? s.charAt(i + 1) : 0;

            if (!inDouble && c == '\'') {
                if (inSingle) {
                    if (next == '\'') {
                        i++; // skip escaped '
                    } else {
                        inSingle = false;
                    }
                } else {
                    inSingle = true;
                }
                continue;
            }
            if (!inSingle && c == '"') {
                if (inDouble) {
                    if (next == '"') {
                        i++; // skip escaped "
                    } else {
                        inDouble = false;
                    }
                } else {
                    inDouble = true;
                }
                continue;
            }
            if (inSingle || inDouble) continue;

            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
        }
        throw new RuntimeException("括號不平衡，找不到對應 ')'");
    }

    /**
     * 將頂層逗號分隔的清單拆分，忽略括號/字串/雙引號內的逗號
     * 
     * 重要：在 VALUES 部分，雙引號應該只在標識符中使用（實際上VALUES中很少用到）
     * 字符串值中的雙引號（如 HTML 內容）應該被當作普通字符處理
     */
    protected static List<String> splitTopLevel(String region) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inSingle = false;  // 單引號字符串內
        int depth = 0;  // 括號深度
        
        for (int i = 0; i < region.length(); i++) {
            char c = region.charAt(i);
            char next = (i + 1 < region.length()) ? region.charAt(i + 1) : 0;

            // 處理單引號字符串（這是最重要的）
            if (c == '\'') {
                cur.append(c);
                if (inSingle) {
                    // 檢查是否為轉義的單引號 ''
                    if (next == '\'') {
                        cur.append(next);
                        i++;  // 跳過下一個單引號
                    } else {
                        // 字符串結束
                        inSingle = false;
                    }
                } else {
                    // 字符串開始
                    inSingle = true;
                }
                continue;
            }

            // 如果在單引號字符串內，所有字符都直接添加（包括雙引號、逗號等）
            if (inSingle) {
                cur.append(c);
                continue;
            }

            // 在字符串外部，處理括號和逗號
            if (c == '(') {
                depth++;
                cur.append(c);
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
                cur.append(c);
            } else if (c == ',' && depth == 0) {
                // 頂層逗號，分割值
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        
        if (cur.length() > 0) {
            out.add(cur.toString().trim());
        }
        
        return out;
    }

    /**
     * 去掉識別字雙引號包裝
     */
    private static String unquoteIdentifier(String s) {
        String t = s.trim();
        if (t.length() >= 2 && t.charAt(0) == '"' && t.charAt(t.length() - 1) == '"') {
            // 將 "" 轉回 "
            return t.substring(1, t.length() - 1).replace("\"\"", "\"");
        }
        return t;
    }

    /**
     * 尋找下一個非 ')' 的字元（容錯），若第一個就是 '(' 則直接回傳該位置
     */
    private static int nextChar(String s, char target, int start) {
        return start;
    }
}
