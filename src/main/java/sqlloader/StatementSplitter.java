package sqlloader;

import java.util.function.Consumer;

/**
 * 將輸入字元流切成 SQL 語句（以 ';' 作為結尾），
 * 但需避免位於字串（單引號/雙引號）、括號或註解內的分號。
 * <p>
 * 支援：
 * - 單引號字串（'' 逃脫單引號）
 * - 雙引號識別字（"" 逃脫雙引號）
 * - 單行註解：--
 * - 區塊註解：/* *\/
 * <p>
 * 不會移除字面值內容；僅不把註解中的 ';' 認作結束。
 * 會「移除」註解內容本身（輸出語句將不含註解），以避免雜訊。
 *
 * @author Cheng
 * @since 2025-08-28 15:24
 **/
public class StatementSplitter {
    private final StringBuilder sb = new StringBuilder();

    private boolean inSingleQuote = false;
    private boolean inDoubleQuote = false;
    private boolean inLineComment = false;
    private boolean inBlockComment = false;
    private int parenDepth = 0;
    private char prev = 0;

    public void accept(char[] buf, int off, int len, Consumer<String> onStatement) {
        int end = off + len;
        for (int i = off; i < end; i++) {
            char c = buf[i];
            char next = (i + 1 < end) ? buf[i + 1] : 0;

            if (inLineComment) {
                if (c == '\r' || c == '\n') {
                    inLineComment = false;
                    append(c);
                } else {
                    // skip comment char
                }
                prev = c;
                continue;
            }
            if (inBlockComment) {
                if (prev == '*' && c == '/') {
                    inBlockComment = false;
                    // do not append the trailing '/'
                    c = 0;
                }
                prev = c;
                continue;
            }

            // detect comment start (only when not in quotes)
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '-' && next == '-') {
                    inLineComment = true;
                    i++; // consume next '-'
                    prev = 0;
                    continue;
                }
                if (c == '/' && next == '*') {
                    inBlockComment = true;
                    i++; // consume '*'
                    prev = 0;
                    continue;
                }
            }

            // quotes handling
            if (!inDoubleQuote && c == '\'') {
                if (inSingleQuote) {
                    // if next is another single quote, it's escaped quote inside string
                    if (next == '\'') {
                        append(c); // append one '
                        i++;       // skip the escape '
                        append('\'');
                    } else {
                        inSingleQuote = false;
                        append(c);
                    }
                } else {
                    inSingleQuote = true;
                    append(c);
                }
                prev = c;
                continue;
            }

            if (!inSingleQuote && c == '"') {
                if (inDoubleQuote) {
                    if (next == '"') {
                        append(c);
                        i++;
                        append('"');
                    } else {
                        inDoubleQuote = false;
                        append(c);
                    }
                } else {
                    inDoubleQuote = true;
                    append(c);
                }
                prev = c;
                continue;
            }

            // parentheses depth (only when not in quotes)
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') parenDepth++;
                else if (c == ')') parenDepth = Math.max(0, parenDepth - 1);
            }

            // statement termination
            if (!inSingleQuote && !inDoubleQuote && parenDepth == 0 && c == ';') {
                String out = sb.toString();
                sb.setLength(0);
                prev = c;
                onStatement.accept(out);
                continue;
            }

            append(c);
            prev = c;
        }
    }

    /**
     * 呼叫於輸入結束時，將緩衝區中未以分號結尾的語句一併送出。
     */
    public void finish(Consumer<String> onStatement) {
        String out = sb.toString().trim();
        if (!out.isEmpty()) {
            onStatement.accept(out);
        }
        sb.setLength(0);
        // 重置狀態，避免重複使用造成影響
        inSingleQuote = false;
        inDoubleQuote = false;
        inLineComment = false;
        inBlockComment = false;
        parenDepth = 0;
        prev = 0;
    }

    private void append(char c) {
        if (c != 0) sb.append(c);
    }
}
