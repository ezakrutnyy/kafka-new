package service;

import kafka.value.Employee;
import org.apache.poi.common.usermodel.fonts.FontFamily;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SXSSFExportExcelService {

    final public static SimpleDateFormat formatDate = new SimpleDateFormat("dd.MM.yyyy");

    private static final Logger logger = LoggerFactory.getLogger(SXSSFExportExcelService.class);

    private static final int FLUSH_ROWS_COUNT = 100;

    private static final int COLUMNS_NUMBER_TO_AUTO_SIZE = 60;

    private final SXSSFWorkbook wb;

    private final SXSSFSheet sheet;

    public SXSSFExportExcelService() {
        wb = new SXSSFWorkbook(-1);
        /*temp files will be gzipped*/
        wb.setCompressTempFiles(true);

        sheet = wb.createSheet(formatDate.format(new Date()));
        /* For Autosize columns*/
        sheet.trackAllColumnsForAutoSizing();
    }

    public void export(List<String> titles, List<Employee> employees) throws IOException {
        Path path = Paths.get("src/main/resources/report/employees.xlsx");
        try {
            Files.delete(path);
        } catch (Exception ex) {
            logger.error("File is deleted!");
        }

        // Заголовки
        Row rowTitle = sheet.createRow(0);
        AtomicInteger counter = new AtomicInteger();

        // Стиль для заголовка
        CellStyle titleStyle = wb.createCellStyle();

        Font titleFont = wb.createFont();
        titleFont.setBold(true);
        titleFont.setFontName(FontFamily.FF_ROMAN.name());
        titleFont.setColor(IndexedColors.BLACK.index);
        titleStyle.setFont(titleFont);

        titleStyle.setAlignment(HorizontalAlignment.CENTER);
        titleStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        titleStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        titleStyle.setFillForegroundColor(IndexedColors.LIGHT_GREEN.index);


        // Стиль для body строк
        CellStyle bodyStyle = wb.createCellStyle();
        bodyStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        bodyStyle.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.index);
        bodyStyle.setAlignment(HorizontalAlignment.CENTER);
        bodyStyle.setVerticalAlignment(VerticalAlignment.CENTER);

        titles.forEach(title -> {
            Cell cell = rowTitle.createCell(counter.getAndIncrement());
            cell.setCellValue(title);
            cell.setCellStyle(titleStyle);
        });

        try (OutputStream outputStream = Files.newOutputStream(path)) {
            try {
                AtomicInteger counterRow = new AtomicInteger(1);
                employees.forEach(employee -> {
                    /*Flush data on disk*/
                    if (counterRow.get() % FLUSH_ROWS_COUNT == 0) {
                        try {
                            sheet.flushRows(FLUSH_ROWS_COUNT);
                            logger.debug("Flush data on disk");
                        } catch (IOException e) {
                            logger.error("Could not write data output to disk due to error", e);
                        }
                    }

                    Row rowBody = sheet.createRow(counterRow.getAndIncrement());
                    Cell cell = rowBody.createCell(0);
                    cell.setCellValue(employee.getLogin());
                    cell.setCellStyle(bodyStyle);

                    cell = rowBody.createCell(1);
                    cell.setCellValue(employee.getCountry());
                    cell.setCellStyle(bodyStyle);

                    cell = rowBody.createCell(2);
                    cell.setCellValue(employee.getCity());
                    cell.setCellStyle(bodyStyle);

                    cell = rowBody.createCell(3);
                    cell.setCellValue(employee.getEmail());
                    cell.setCellStyle(bodyStyle);

                    cell = rowBody.createCell(4);
                    cell.setCellValue(employee.getAge());
                    cell.setCellStyle(bodyStyle);
                });
            } finally {
                autoSizeSheet();
                wb.write(outputStream);
                wb.close();
                /* dispose of temporary files backing this workbook on disk*/
                wb.dispose();
            }
        }
    }

    private void autoSizeSheet() {
        for (int i = 0; i < COLUMNS_NUMBER_TO_AUTO_SIZE; i++) {
            sheet.autoSizeColumn(i);
        }
    }
}