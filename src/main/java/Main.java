import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      return;
    }

    String databaseFilePath = args[0];
    String command = args[1];

    try (RandomAccessFile file = new RandomAccessFile(databaseFilePath, "r")) {
      // Read page size from header (offset 16, 2 bytes)
      file.seek(16);
      int pageSize = file.readUnsignedShort();

      if (command.equals(".dbinfo")) {
        // Get cell count from page 1 header (offset 100 + 3 = 103)
        file.seek(103);
        int numberOfTables = file.readUnsignedShort();
        System.out.println("database page size: " + pageSize);
        System.out.println("number of tables: " + numberOfTables);
      } else if (command.toUpperCase().startsWith("SELECT COUNT(*)")) {
        // Parse table name from SQL
        Pattern pattern = Pattern.compile("SELECT\\s+COUNT\\(\\*\\)\\s+FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(command);
        if (!matcher.find()) {
          System.out.println("Invalid SQL command");
          return;
        }
        String tableName = matcher.group(1);

        // Find the table's root page from sqlite_schema
        TableInfo tableInfo = findTableInfo(file, pageSize, tableName);
        if (tableInfo == null) {
          System.out.println("Table not found: " + tableName);
          return;
        }

        // Count cells on the root page
        int rowCount = countCellsOnPage(file, pageSize, tableInfo.rootPage);
        System.out.println(rowCount);
      } else if (command.toUpperCase().startsWith("SELECT")) {
        // Parse SELECT columns FROM table [WHERE column = 'value']
        Pattern pattern = Pattern.compile("SELECT\\s+([\\w,\\s]+)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(\\w+)\\s*=\\s*'([^']*)')?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(command);
        if (!matcher.find()) {
          System.out.println("Invalid SQL command");
          return;
        }
        String columnsStr = matcher.group(1).trim();
        String tableName = matcher.group(2).trim();
        String whereColumn = matcher.group(3);
        String whereValue = matcher.group(4);

        // Find the table info from sqlite_schema
        TableInfo tableInfo = findTableInfo(file, pageSize, tableName);
        if (tableInfo == null) {
          System.out.println("Table not found: " + tableName);
          return;
        }

        // Parse column names and find their indices
        String[] columnNames = columnsStr.split("\\s*,\\s*");
        int[] columnIndices = new int[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
          columnIndices[i] = findColumnIndex(tableInfo.sql, columnNames[i].trim());
          if (columnIndices[i] == -1) {
            System.out.println("Column not found: " + columnNames[i]);
            return;
          }
        }

        // Parse WHERE clause if present
        int whereColumnIndex = -1;
        if (whereColumn != null) {
          whereColumnIndex = findColumnIndex(tableInfo.sql, whereColumn);
          if (whereColumnIndex == -1) {
            System.out.println("Column not found: " + whereColumn);
            return;
          }
        }

        // Read all rows and extract the column values
        List<String[]> rows = readColumnValues(file, pageSize, tableInfo.rootPage, columnIndices, whereColumnIndex, whereValue);
        for (String[] row : rows) {
          System.out.println(String.join("|", row));
        }
      } else {
        System.out.println("Missing or invalid command passed: " + command);
      }
    } catch (IOException e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }

  static class TableInfo {
    int rootPage;
    String sql;
    TableInfo(int rootPage, String sql) {
      this.rootPage = rootPage;
      this.sql = sql;
    }
  }

  // Read a varint from the file and return both the value and bytes consumed
  private static long[] readVarint(RandomAccessFile file) throws IOException {
    long result = 0;
    int bytesRead = 0;
    for (int i = 0; i < 9; i++) {
      int b = file.read();
      bytesRead++;
      if (i < 8) {
        result = (result << 7) | (b & 0x7F);
        if ((b & 0x80) == 0) break;
      } else {
        result = (result << 8) | b;
      }
    }
    return new long[]{result, bytesRead};
  }

  // Read a varint from a byte array at a given offset
  private static long[] readVarintFromBytes(byte[] data, int offset) {
    long result = 0;
    int bytesRead = 0;
    for (int i = 0; i < 9 && offset + i < data.length; i++) {
      int b = data[offset + i] & 0xFF;
      bytesRead++;
      if (i < 8) {
        result = (result << 7) | (b & 0x7F);
        if ((b & 0x80) == 0) break;
      } else {
        result = (result << 8) | b;
      }
    }
    return new long[]{result, bytesRead};
  }

  private static int findColumnIndex(String createSql, String columnName) {
    // Parse CREATE TABLE statement to find column index
    // Format: CREATE TABLE tablename (col1 type, col2 type, ...)
    Pattern pattern = Pattern.compile("\\((.+)\\)", Pattern.DOTALL);
    Matcher matcher = pattern.matcher(createSql);
    if (!matcher.find()) return -1;

    String columnsDef = matcher.group(1);
    String[] columns = columnsDef.split(",");
    
    for (int i = 0; i < columns.length; i++) {
      String col = columns[i].trim();
      // Get the column name (first word)
      String[] parts = col.split("\\s+");
      if (parts.length > 0 && parts[0].equalsIgnoreCase(columnName)) {
        return i;
      }
    }
    return -1;
  }

  private static TableInfo findTableInfo(RandomAccessFile file, int pageSize, String tableName) throws IOException {
    // Page 1 starts at offset 0, but has 100-byte file header
    long pageOffset = 0;
    int headerOffset = 100; // Skip file header for page 1

    // Read page header
    file.seek(pageOffset + headerOffset);
    int pageType = file.read();
    file.skipBytes(2); // first freeblock
    int cellCount = file.readUnsignedShort();
    int cellContentStart = file.readUnsignedShort();
    file.skipBytes(1); // fragmented free bytes

    // For interior pages, skip the right-most pointer (4 bytes)
    if (pageType == 0x05 || pageType == 0x02) {
      file.skipBytes(4);
    }

    // Read cell pointer array
    int[] cellPointers = new int[cellCount];
    for (int i = 0; i < cellCount; i++) {
      cellPointers[i] = file.readUnsignedShort();
    }

    // Parse each cell to find the table
    for (int i = 0; i < cellCount; i++) {
      file.seek(pageOffset + cellPointers[i]);

      // Read payload size (varint)
      long[] payloadResult = readVarint(file);
      long payloadSize = payloadResult[0];

      // Read rowid (varint)
      long[] rowidResult = readVarint(file);

      // Read record header
      long recordStart = file.getFilePointer();
      long[] headerSizeResult = readVarint(file);
      long headerSize = headerSizeResult[0];

      // Read serial types from header
      List<Long> serialTypes = new ArrayList<>();
      long headerBytesRead = headerSizeResult[1];
      while (headerBytesRead < headerSize) {
        long[] serialResult = readVarint(file);
        serialTypes.add(serialResult[0]);
        headerBytesRead += serialResult[1];
      }

      // Position at start of column values
      file.seek(recordStart + headerSize);

      // sqlite_schema columns: type, name, tbl_name, rootpage, sql
      // We need: type (0), name (1), rootpage (3), sql (4)
      String type = null;
      String name = null;
      int rootPage = 0;
      String sql = null;

      for (int col = 0; col < serialTypes.size(); col++) {
        long serialType = serialTypes.get(col);
        
        if (serialType == 0) {
          // NULL
          continue;
        } else if (serialType >= 1 && serialType <= 6) {
          // Integer
          int intSize = serialType == 1 ? 1 : serialType == 2 ? 2 : serialType == 3 ? 3 : 
                        serialType == 4 ? 4 : serialType == 5 ? 6 : 8;
          long value = 0;
          for (int b = 0; b < intSize; b++) {
            value = (value << 8) | file.read();
          }
          if (col == 3) rootPage = (int) value;
        } else if (serialType >= 13 && serialType % 2 == 1) {
          // Text
          int strLen = (int) ((serialType - 13) / 2);
          byte[] strBytes = new byte[strLen];
          file.readFully(strBytes);
          String strValue = new String(strBytes);
          if (col == 0) type = strValue;
          else if (col == 1) name = strValue;
          else if (col == 4) sql = strValue;
        } else if (serialType >= 12 && serialType % 2 == 0) {
          // Blob - skip
          int blobLen = (int) ((serialType - 12) / 2);
          file.skipBytes(blobLen);
        }
      }

      if ("table".equals(type) && tableName.equalsIgnoreCase(name)) {
        return new TableInfo(rootPage, sql);
      }
    }
    return null;
  }

  private static List<String[]> readColumnValues(RandomAccessFile file, int pageSize, int pageNumber, int[] targetColumnIndices, int whereColumnIndex, String whereValue) throws IOException {
    List<String[]> rows = new ArrayList<>();
    
    // Pages are 1-indexed, so page N starts at offset (N-1) * pageSize
    long pageOffset = (long) (pageNumber - 1) * pageSize;
    
    // Page 1 has 100-byte file header, other pages don't
    int headerOffset = (pageNumber == 1) ? 100 : 0;

    // Read page header
    file.seek(pageOffset + headerOffset);
    int pageType = file.read();
    file.skipBytes(2); // first freeblock
    int cellCount = file.readUnsignedShort();
    file.skipBytes(3); // cell content start + fragmented free bytes

    // For interior pages, skip the right-most pointer (4 bytes)
    if (pageType == 0x05 || pageType == 0x02) {
      file.skipBytes(4);
    }

    // Read cell pointer array
    int[] cellPointers = new int[cellCount];
    for (int i = 0; i < cellCount; i++) {
      cellPointers[i] = file.readUnsignedShort();
    }

    // Parse each cell
    for (int i = 0; i < cellCount; i++) {
      file.seek(pageOffset + cellPointers[i]);

      // Read payload size (varint)
      long[] payloadResult = readVarint(file);

      // Read rowid (varint)
      long[] rowidResult = readVarint(file);

      // Read record header
      long recordStart = file.getFilePointer();
      long[] headerSizeResult = readVarint(file);
      long headerSize = headerSizeResult[0];

      // Read serial types from header
      List<Long> serialTypes = new ArrayList<>();
      long headerBytesRead = headerSizeResult[1];
      while (headerBytesRead < headerSize) {
        long[] serialResult = readVarint(file);
        serialTypes.add(serialResult[0]);
        headerBytesRead += serialResult[1];
      }

      // Position at start of column values
      file.seek(recordStart + headerSize);

      // Read all column values
      String[] columnValues = new String[serialTypes.size()];
      for (int col = 0; col < serialTypes.size(); col++) {
        long serialType = serialTypes.get(col);
        
        if (serialType == 0) {
          // NULL
          columnValues[col] = "";
        } else if (serialType >= 1 && serialType <= 6) {
          // Integer
          int intSize = serialType == 1 ? 1 : serialType == 2 ? 2 : serialType == 3 ? 3 : 
                        serialType == 4 ? 4 : serialType == 5 ? 6 : 8;
          long value = 0;
          for (int b = 0; b < intSize; b++) {
            value = (value << 8) | file.read();
          }
          columnValues[col] = String.valueOf(value);
        } else if (serialType == 7) {
          // Float (8 bytes)
          byte[] floatBytes = new byte[8];
          file.readFully(floatBytes);
          double d = ByteBuffer.wrap(floatBytes).getDouble();
          columnValues[col] = String.valueOf(d);
        } else if (serialType == 8) {
          // Integer 0
          columnValues[col] = "0";
        } else if (serialType == 9) {
          // Integer 1
          columnValues[col] = "1";
        } else if (serialType >= 12 && serialType % 2 == 0) {
          // Blob
          int blobLen = (int) ((serialType - 12) / 2);
          byte[] blobBytes = new byte[blobLen];
          file.readFully(blobBytes);
          columnValues[col] = new String(blobBytes);
        } else if (serialType >= 13 && serialType % 2 == 1) {
          // Text
          int strLen = (int) ((serialType - 13) / 2);
          byte[] strBytes = new byte[strLen];
          file.readFully(strBytes);
          columnValues[col] = new String(strBytes);
        }
      }
      
      // Apply WHERE filter if present
      if (whereColumnIndex >= 0 && whereValue != null) {
        if (!columnValues[whereColumnIndex].equals(whereValue)) {
          continue; // Skip this row
        }
      }
      
      // Extract only the requested columns
      String[] rowValues = new String[targetColumnIndices.length];
      for (int j = 0; j < targetColumnIndices.length; j++) {
        rowValues[j] = columnValues[targetColumnIndices[j]];
      }
      rows.add(rowValues);
    }

    return rows;
  }

  private static int countCellsOnPage(RandomAccessFile file, int pageSize, int pageNumber) throws IOException {
    // Pages are 1-indexed, so page N starts at offset (N-1) * pageSize
    long pageOffset = (long) (pageNumber - 1) * pageSize;
    
    // Page 1 has 100-byte file header, other pages don't
    int headerOffset = (pageNumber == 1) ? 100 : 0;

    // Read cell count from page header (offset 3-4 within page header)
    file.seek(pageOffset + headerOffset + 3);
    return file.readUnsignedShort();
  }
}
