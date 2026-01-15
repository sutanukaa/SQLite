import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args){
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      return;
    }

    String databaseFilePath = args[0];
    String command = args[1];

    switch (command) {
      case ".dbinfo" -> {
        try {
          FileInputStream databaseFile = new FileInputStream(new File(databaseFilePath));
          
          databaseFile.skip(16); // Skip the first 16 bytes of the header
          byte[] pageSizeBytes = new byte[2]; // The following 2 bytes are the page size
          databaseFile.read(pageSizeBytes);
          short pageSizeSigned = ByteBuffer.wrap(pageSizeBytes).getShort();
          int pageSize = Short.toUnsignedInt(pageSizeSigned);

          // Skip to the page header (at offset 100, after the 100-byte file header)
          // We've already read 18 bytes (16 skipped + 2 for page size), so skip 82 more
          databaseFile.skip(82);
          
          // Skip the first 3 bytes of the page header (page type + first freeblock offset)
          databaseFile.skip(3);
          
          // Read the 2-byte cell count (number of tables)
          byte[] cellCountBytes = new byte[2];
          databaseFile.read(cellCountBytes);
          int numberOfTables = Short.toUnsignedInt(ByteBuffer.wrap(cellCountBytes).getShort());

          System.out.println("database page size: " + pageSize);
          System.out.println("number of tables: " + numberOfTables);
        } catch (IOException e) {
          System.out.println("Error reading file: " + e.getMessage());
        }
      }
      default -> System.out.println("Missing or invalid command passed: " + command);
    }
  }
}
