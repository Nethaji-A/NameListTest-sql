package queries;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class PostgresQuery2 {

    private static final String URL = "jdbc:postgresql://38.242.220.73:5433/postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "admin123";

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: java -jar myapp.jar <inputFile> <outputDir>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputDir = args[1];

        String sql =
                "WITH input_tokens AS ( " +
                        "    SELECT unnest(string_to_array(lower(?), ' ')) AS token " +
                        "), " +
                        "candidate_rows2 AS ( " +
                        "    SELECT * " +
                        "    FROM sanction_active sa " +
                        "    WHERE sa.type = 'Person' " +
                        "), " +
                        "candidate_rows AS ( " +
                        "    SELECT * " +
                        "    FROM sanction_active sa " +
                        "    WHERE sa.sdnname % ? " +
                        "), " +
                        "matched AS ( " +
                        "    SELECT c.sdnname, " +
                        "           c.sanction_id, " +
                        "           c.sdnname_tokens_count, " +
                        "           i.token AS input_token, " +
                        "           sdn_token, " +
                        "           levenshtein(sdn_token, i.token) AS distance " +
                        "    FROM candidate_rows c " +
                        "    JOIN LATERAL unnest(c.sdnname_tokens) AS sdn_token ON TRUE " +
                        "    JOIN input_tokens i ON levenshtein(sdn_token, i.token) <= 2 " +
                        ") " +
                        "SELECT sdnname, " +
                        "       sanction_id, " +
                        "       sdnname_tokens_count, " +
                        "       COUNT(DISTINCT input_token) AS matched_input_tokens, " +
                        "       (sdnname_tokens_count + ?) - (COUNT(DISTINCT input_token) * 2) AS score_calc " +
                        "FROM matched " +
                        "GROUP BY sdnname, sanction_id, sdnname_tokens_count " +
                        "HAVING (sdnname_tokens_count + ?) - (COUNT(DISTINCT input_token) * 2) <= 3 " +
                        "ORDER BY matched_input_tokens DESC, sdnname_tokens_count ASC " +
                        "LIMIT 3000";


        try {
			FileInputStream fis = new FileInputStream(new File(inputFile));
			Workbook workbook = new XSSFWorkbook(fis);
			Sheet sheet = workbook.getSheetAt(0); 
			Integer times = 1;
            boolean isFirstRow = true;
            for (Row row : sheet) {
                if (isFirstRow) {
                    isFirstRow = false;
                    continue; // skip header
                }
				Cell cell = row.getCell(0); 
				if (cell != null) {
					String inputName = cell.getStringCellValue().trim();
					String safeSheetName = inputName.replaceAll("[\\\\/?*\\[\\]:]", "");
					System.out.println("Processing: " + safeSheetName);
					checkSimiliarity(safeSheetName,sql);
					times++;
				}
			}

			workbook.close();
			fis.close();
			
			System.out.println("Neo4j Results Write Completed In Excel");

		} catch (IOException e) {
			e.printStackTrace();
		}
  
    }
    
    
    
	private static void checkSimiliarity(String inputName, String sql, String outPutDir) {

        List<String[]> results = new ArrayList<>();

		try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
				PreparedStatement stmt = conn.prepareStatement(sql)) {

            String[] tokens = inputName.toLowerCase().split("\\s+"); // split by one or more spaces
            int tokenCount = tokens.length;

			stmt.setString(1, inputName.toLowerCase());  
			stmt.setString(2, inputName.toLowerCase());
            stmt.setInt(3, tokenCount);
            stmt.setInt(4, tokenCount);
			
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String sdnname = rs.getString("sdnname");
					String sanctionId = rs.getString("sanction_id");

					results.add(new String[] { sanctionId, sdnname });
				}
			}

			System.out.println("Query Execution Completed");

		} catch (SQLException e) {
			e.printStackTrace();
		}

		writeResultsToExcel(outPutDir, inputName, results);

	}

	
	
//    private static void writeResultsToExcel(String filePath, String sheetName, List<String[]> results) {
//        try (FileInputStream fis = new FileInputStream(filePath);
//
//             XSSFWorkbook workbook = new XSSFWorkbook(fis)) {
//
//
//            sheetName = sheetName.length() > 31 ? sheetName.substring(0, 31) : sheetName;
//
//            Sheet sheet = workbook.getSheet(sheetName);
//            if (sheet == null) {
//                sheet = workbook.createSheet(sheetName);
//            }
//
//            Row header = sheet.getRow(0);
//            if (header == null) {
//                header = sheet.createRow(0);
//            }
//            if (header.getCell(3) == null) header.createCell(3).setCellValue("Sanction ID - Postgres");
//            if (header.getCell(4) == null) header.createCell(4).setCellValue("SDN Name - Postgres");
//            if (header.getCell(5) == null) header.createCell(5).setCellValue("Compare B with E"); // New column F
//
//            int rowNum = 1;
//            for (String[] rowData : results) {
//                Row row = sheet.getRow(rowNum);
//                if (row == null) {
//                    row = sheet.createRow(rowNum);
//                }
//                row.createCell(3).setCellValue(rowData[0]);
//                row.createCell(4).setCellValue(rowData[1]);
//
//                Cell formulaCell = row.createCell(5);
//                String formula = "COUNTIF(E:E,B" + (rowNum + 1) + ")>0";
//                formulaCell.setCellFormula(formula);
//
//                rowNum++;
//            }
//
//            try (FileOutputStream fos = new FileOutputStream(filePath)) {
//                workbook.write(fos);
//            }
//
//            System.out.println("Postgres results written to Excel with compare column: " + filePath);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
private static void writeResultsToExcel(String filePath, String sheetName, List<String[]> results) {
    try {
        XSSFWorkbook workbook;
        File file = new File(filePath);

        if (file.exists()) {
            // Load existing workbook
            try (FileInputStream fis = new FileInputStream(file)) {
                workbook = new XSSFWorkbook(fis);
            }
        } else {
            // Create new workbook if file not exists
            workbook = new XSSFWorkbook();
        }

        // Ensure sheet name length is within Excel limit
        sheetName = sheetName.length() > 31 ? sheetName.substring(0, 31) : sheetName;

        // Create or get sheet
        Sheet sheet = workbook.getSheet(sheetName);
        if (sheet == null) {
            sheet = workbook.createSheet(sheetName);
        }

        // Create header row if not present
        Row header = sheet.getRow(0);
        if (header == null) {
            header = sheet.createRow(0);
        }
        if (header.getCell(3) == null) header.createCell(3).setCellValue("Sanction ID - Postgres");
        if (header.getCell(4) == null) header.createCell(4).setCellValue("SDN Name - Postgres");
        if (header.getCell(5) == null) header.createCell(5).setCellValue("Compare B with E");

        // Write results
        int rowNum = 1;
        for (String[] rowData : results) {
            Row row = sheet.getRow(rowNum);
            if (row == null) {
                row = sheet.createRow(rowNum);
            }
            row.createCell(3).setCellValue(rowData[0]); // sanction_id
            row.createCell(4).setCellValue(rowData[1]); // sdnname

            // Add formula in column F
            Cell formulaCell = row.createCell(5);
            String formula = "COUNTIF(E:E,B" + (rowNum + 1) + ")>0";
            formulaCell.setCellFormula(formula);

            rowNum++;
        }

        // Save workbook (create file if needed)
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            workbook.write(fos);
        }

        workbook.close();
        System.out.println("Postgres results written to Excel with compare column: " + filePath);

    } catch (IOException e) {
        e.printStackTrace();
    }
}


}
