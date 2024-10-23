package com.example.GT_SP_GET_TKT_EINVOICE_INTER;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.sql.*;

public class GT_SP_FE_TKT_EINVOICE_INTER implements RequestHandler<Input, String> {
    private static final String DB_URL = "jdbc:mysql://your-database-endpoint:3306/your-database-name";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    @Override
    public String handleRequest(Input input, Context context) {
        String V_PROCESSDATE = input.getV_PROCESSDATE();
        String result = processInvoiceData(V_PROCESSDATE);

        return result.isEmpty() ? "No data processed" : result;
    }

    private String processInvoiceData(String V_PROCESSDATE) {
        Connection conn = null;
        String cargaError = "";
        try {
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

            // callProcedure(conn, "CALL _AWS_SP_INS_USE_PROCEDURES('I', 'GT_SP_GET_TKT_EINVOICE_INTER')");

            String updateQuery = "UPDATE E_INVOICE SET KEY_FE = CONCAT(TICKETNUMBER, LEFT(DOC_TYPE,1), PROCESS_DATE, TANSAC_TYPE_CODE) " +
                    "WHERE PROCESS_DATE = ? AND IFNULL(KEY_FE,'') = ''";
            try (PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
                stmt.setString(1, V_PROCESSDATE);
                stmt.executeUpdate();
            }

            // Check for duplicated data
            cargaError = checkForDuplicates(conn, V_PROCESSDATE);

            if (cargaError.isEmpty()) {
                // Process the invoice data if there are no duplicates
                if (!validateInvoiceData(conn, V_PROCESSDATE)) {
                    deleteExistingData(conn, V_PROCESSDATE);
                    insertNewData(conn, V_PROCESSDATE);
                }
            } else {
                // Handle duplicate data error (sending SMS and email)
                handleDuplicateDataError(conn, V_PROCESSDATE, cargaError);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return cargaError;
    }

    private void callProcedure(Connection conn, String procedureCall) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(procedureCall);
        }
    }

    private String checkForDuplicates(Connection conn, String V_PROCESSDATE) throws SQLException {
        String cargaError = "";
        String query = "SELECT IFNULL(GROUP_CONCAT(CONCAT(KEY_FE, ' ', SEQ_REG) SEPARATOR '<br>'),'') AS carga_error " +
                "FROM (SELECT KEY_FE, SEQ_REG, COUNT(*) " +
                "FROM E_INVOICE " +
                "WHERE PROCESS_DATE = ? " +
                "AND TICKETNUMBER IN (...) " +  // Add the specific logic for filtering TICKETNUMBER
                "GROUP BY KEY_FE, SEQ_REG HAVING COUNT(*) > 1) X";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, V_PROCESSDATE);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    cargaError = rs.getString("carga_error");
                }
            }
        }
        return cargaError;
    }

    private boolean validateInvoiceData(Connection conn, String V_PROCESSDATE) throws SQLException {
        // Validate data totals and return true if data is correct, false otherwise
        int cantTotal, cantActual;
        String totalQuery = "SELECT COUNT(*) AS total FROM E_INVOICE_INTER WHERE COUNTRY_CALC = 'GT' AND PROCESS_DATE_INTER = ?";
        String actualQuery = "SELECT COUNT(*) AS actual FROM E_INVOICE WHERE PROCESS_DATE = ? AND TICKETNUMBER IN (...)";  // Add filter logic

        try (PreparedStatement totalStmt = conn.prepareStatement(totalQuery);
             PreparedStatement actualStmt = conn.prepareStatement(actualQuery)) {

            totalStmt.setString(1, V_PROCESSDATE);
            actualStmt.setString(1, V_PROCESSDATE);

            try (ResultSet totalRs = totalStmt.executeQuery();
                 ResultSet actualRs = actualStmt.executeQuery()) {

                if (totalRs.next() && actualRs.next()) {
                    cantTotal = totalRs.getInt("total");
                    cantActual = actualRs.getInt("actual");
                    return cantTotal == cantActual;
                }
            }
        }
        return false;
    }

    private void deleteExistingData(Connection conn, String V_PROCESSDATE) throws SQLException {
        String deleteQuery = "DELETE FROM E_INVOICE_INTER WHERE PROCESS_DATE_INTER = ? AND COUNTRY_CALC = 'GT'";
        try (PreparedStatement stmt = conn.prepareStatement(deleteQuery)) {
            stmt.setString(1, V_PROCESSDATE);
            stmt.executeUpdate();
        }
    }

    private void insertNewData(Connection conn, String V_PROCESSDATE) throws SQLException {
        String insertQuery = "INSERT INTO E_INVOICE_INTER (COUNTRY_CALC, KEY_FE, SEQ_REG, TYPE_INTER, PROCESS_DATE_INTER, TICKETNUMBER) " +
                "SELECT 'GT', TRIM(REPLACE(REPLACE(KEY_FE,'\\r',''), '\\n','')), SEQ_REG, " +
                "(CASE WHEN TYPE_REG IN ('T','Z') THEN TYPE_REG ELSE '' END), PROCESS_DATE, TICKETNUMBER " +
                "FROM E_INVOICE WHERE PROCESS_DATE = ? AND TICKETNUMBER IN (...)";  // Add filter logic

        try (PreparedStatement stmt = conn.prepareStatement(insertQuery)) {
            stmt.setString(1, V_PROCESSDATE);
            stmt.executeUpdate();
        }
    }

    private void handleDuplicateDataError(Connection conn, String V_PROCESSDATE, String cargaError) throws SQLException {
        String smsQuery = "SELECT FN_SEND_SMS('+51993047545', CONCAT('DATA DUPLICADA GT PARA EL ', ?, '<br><br>', ?)) INTO @response";
        String mailQuery = "SELECT FN_SEND_MAIL_ALERTMIATECH('maloisx@gmail.com|mvillanueva@miatech.net', " +
                "CONCAT('ERROR CARGA DATA GT ', ?), CONCAT('DATA DUPLICADA PARA EL ', ?, '<br><br>', ?), '') INTO @mensaje";

        try (PreparedStatement smsStmt = conn.prepareStatement(smsQuery);
             PreparedStatement mailStmt = conn.prepareStatement(mailQuery)) {

            smsStmt.setString(1, V_PROCESSDATE);
            smsStmt.setString(2, cargaError);
            smsStmt.executeQuery();

            mailStmt.setString(1, V_PROCESSDATE);
            mailStmt.setString(2, V_PROCESSDATE);
            mailStmt.setString(3, cargaError);
            mailStmt.executeQuery();
        }
    }
}
