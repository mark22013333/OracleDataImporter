# Oracle SQL 批次匯入工具

一個專門為 Oracle 資料庫設計的 SQL 檔案批次匯入工具，支援大檔案處理和中文時間戳格式。

## ✨ 功能特色

- 🚀 **高效能串流處理** - 支援 1GB+ 大檔案，記憶體使用量低
- 📅 **中文時間戳支援** - 自動處理 `to_timestamp('19-8月 -25 10.30.00.000000000 上午','DD-MON-RR HH.MI.SSXFF AM')` 格式
- 🔧 **智慧批次處理** - 自動偵測長字串並最佳化執行策略
- 🛡️ **錯誤處理** - 可選擇遇錯繼續或停止執行
- 📊 **進度顯示** - 即時顯示讀取和寫入進度
- 🎯 **主鍵處理** - 可選擇性忽略特定欄位（如 WCSID）
- 🌐 **多編碼支援** - 支援各種檔案編碼格式
- 📦 **自動依賴管理** - 使用 Gradle 自動下載所需驅動程式

## 📋 系統需求

- Java 8 或更高版本
- Gradle (專案已包含 gradle wrapper，無需額外安裝)
- Oracle 資料庫連線權限

## 🚀 快速開始

### 1. 構建專案

```bash
# 使用 gradle wrapper 構建專案（自動下載依賴）
# Windows
gradlew.bat clean shadowJar

# Linux/Mac
./gradlew clean shadowJar
```

構建完成後會在 `build/libs/` 目錄下生成 `OracleJDBC.jar` 檔案。

### 2. 基本使用

```bash
# 基本匯入指令
java -jar "OracleJDBC.jar" \
  --jdbcUrl "jdbc:oracle:thin:@//your-host:1521/your-service" \
  --user "your-username" \
  --password "your-password" \
  --file "path/to/your/file.sql"
```

## 📖 詳細使用說明

### 基本參數說明

| 參數 | 必填 | 說明 | 範例 |
|------|------|------|------|
| `--jdbcUrl` | ✅ | Oracle 資料庫連線字串 | `jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1` |
| `--user` | ✅ | 資料庫使用者名稱 | `SCOTT` |
| `--password` | ❌ | 資料庫密碼（未提供時會提示輸入） | `tiger` |
| `--file` | ✅ | 要匯入的 SQL 檔案路徑 | `D:\data\import.sql` |

### 進階參數

| 參數 | 預設值 | 說明 |
|------|--------|------|
| `--batchSize` | 500 | 批次執行大小，數字越大效能越好但記憶體使用越高 |
| `--ignorePK` | false | 是否忽略指定主鍵欄位（如 WCSID） |
| `--pkName` | WCSID | 要忽略的主鍵欄位名稱（不分大小寫） |
| `--charset` | UTF-8 | SQL 檔案的編碼格式 |
| `--continueOnError` | false | 遇到錯誤時是否繼續執行後續語句 |

### 完整範例

```bash
# Windows 範例
java -jar "OracleJDBC.jar" ^
  --jdbcUrl "jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1" ^
  --user "AP_TAX" ^
  --password "123456" ^
  --file "D:\data\wcst172.sql" ^
  --batchSize 1000 ^
  --ignorePK true ^
  --pkName "WCSID" ^
  --charset "Big5" ^
  --continueOnError true

# Linux/Mac 範例
java -jar "OracleJDBC.jar" \
  --jdbcUrl "jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1" \
  --user "AP_TAX" \
  --password "123456" \
  --file "/data/wcst172.sql" \
  --batchSize 1000 \
  --ignorePK true \
  --pkName "WCSID" \
  --charset "Big5" \
  --continueOnError true
```

### 密碼安全提示

為了安全性，建議不要在命令列中直接輸入密碼：

```bash
# 不指定密碼，程式會提示輸入
java -jar "OracleJDBC.jar" \
  --jdbcUrl "jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1" \
  --user "AP_TAX" \
  --file "D:\data\wcst172.sql"
```

## 🔧 支援的時間戳格式

本工具自動支援以下時間戳格式：

### 標準格式
```sql
TIMESTAMP '2025-08-29 10:30:00'
DATE '2025-08-29 10:30:00'
```

### 中文月份格式
```sql
to_timestamp('19-8月 -25 10.30.00.000000000 上午','DD-MON-RR HH.MI.SSXFF AM')
to_timestamp('24-1月-24 10.30.00.000000000 下午','DD-MON-RR HH.MI.SSXFF AM','NLS_DATE_LANGUAGE=TRADITIONAL CHINESE')
```

## 📊 執行輸出範例

```
開始預掃描（僅計算語句數）... 檔案大小：1,234,567 bytes
預掃描完成，偵測到 INSERT 語句總數：約 50,000 條
開始匯入 ...
讀取進度： 45%（555,555/1,234,567 bytes） | 寫入進度： 42%（21,000/50,000）
=== 匯入完成 ===
成功：49,850 條；失敗：150 條；總語句：50,000 條
耗時：約 3 分 45 秒
```

## 🚨 常見問題

### Q: 構建時出現 "無法下載依賴" 錯誤
A: 請檢查：
- 網路連線是否正常
- 公司防火牆是否允許下載 Maven 依賴
- 可以嘗試使用公司內部的 Maven 代理

### Q: 執行時出現 "找不到主類別" 錯誤
A: 請確保：
- 已正確執行 `gradlew clean shadowJar` 構建專案
- 使用 `java -jar "OracleJDBC.jar"` 執行，不是直接執行 class 檔案

### Q: 中文時間戳解析失敗
A: 請確認時間戳格式符合支援的格式，或使用 `NLS_DATE_LANGUAGE=TRADITIONAL CHINESE` 參數

### Q: 記憶體不足
A: 可以調整批次大小 `--batchSize` 參數，建議從 100 開始測試

### Q: 連線失敗
A: 請檢查：
- 資料庫服務是否正在執行
- 連線字串是否正確
- 防火牆設定
- 使用者帳號權限是否足夠

## 🛠️ 開發資訊

### 專案結構
```
├── build.gradle                 # Gradle 構建檔案
├── settings.gradle              # Gradle 設定檔案
├── gradlew                      # Linux/Mac Gradle wrapper
├── gradlew.bat                  # Windows Gradle wrapper
├── src/main/java/sqlloader/
│   ├── SqlBatchLoader.java      # 主程式
│   ├── SqlInsertRewriter.java   # SQL 重寫工具
│   └── StatementSplitter.java   # SQL 語句分割器
└── build/libs/                  # 構建輸出目錄
    └── OracleJDBC.jar          # 最終執行檔
```

### 開發與測試
```bash
# 構建專案（包含所有依賴）
./gradlew clean shadowJar

# 執行測試
./gradlew test

# 查看可用參數
java -jar "build/libs/OracleJDBC.jar" --help
```

### 依賴管理
專案使用 Gradle 管理依賴，主要依賴包括：
- **Oracle JDBC Driver** (ojdbc8:19.20.0.0) - Oracle 資料庫連線驅動
- **ShadowJar Plugin** - 打包所有依賴為單一 JAR 檔案
- **JUnit 5** - 單元測試框架

## 📄 授權條款

本專案僅供內部使用，請遵循公司的資料安全政策。

## 🤝 貢獻

歡迎提交問題反饋和改進建議！

## 📞 技術支援

如有任何問題，請聯繫開發團隊或查看相關技術文件。