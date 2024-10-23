package com.example.GT_SP_FE_CANCELAR_TKT;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class GT_SP_FE_CANCELAR_TKT implements RequestHandler<Input, String> {
    private static final String DB_URL = "jdbc:mysql://your-database-endpoint:3306/your-database-name";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";

    @Override
    public String handleRequest(Input input, Context context) {
        String p_Uuid = input.getP_Uuid();
        String p_User_Cancel = input.getP_User_Cancel();
        String va_Status = executeQuery(p_Uuid);

        if (va_Status.equals("1") || va_Status.equals("2")) {
            return callLaFeTimbrarboletoCancelar(p_Uuid, p_User_Cancel);
        } else if (va_Status.equals("0")) {
            return "OK";
        } else {
            return "Status desconocido";
        }
    }

    private String executeQuery(String p_Uuid){
        String result = "";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            String query = "SELECT TX_STATU_CANCELA FROM XMLID_INTER WHERE TX_UUID = ?";
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            preparedStatement.setString(1, p_Uuid);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                result = resultSet.getString("TX_STATU_CANCELA");
            }
        } catch (SQLException e) {
            System.out.println("Error al conectarse a la base de datos: " + e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println("Error al cerrar la conexión: " + e.getMessage());
            }
        }
        return result.isEmpty() ? "No se encontraron resultados" : result;
    }

    public String invokeLambda(LambdaClient awsLambda, String functionName, String payload) {
        InvokeResponse res = null;
        try {
            InvokeRequest request = InvokeRequest.builder()
                    .functionName(functionName)
                    .payload(SdkBytes.fromUtf8String(payload))
                    .build();

            res = awsLambda.invoke(request);
            return res.payload().asUtf8String();
        } catch (LambdaException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return "ERROR";
        }
    }

    public String callLaFeTimbrarboletoCancelar(String p_Uuid, String p_User_Cancel){
        Region region = Region.EU_WEST_2;
        LambdaClient awsLambda = LambdaClient.builder()
                .region(region)
                .build();

        String va_response = invokeLambda(awsLambda, "arn:aws:lambda:us-east-1:119260075650:function:la_fe_timbrarboleto_cancelar_gt", "\"" + p_Uuid + "\"")
                .replace("\"", "")
                .replace("\n", "")
                .replace("{", "")
                .replace("}", "");

        Map<String, String> keyValueMap = new HashMap<>();
        String[] pairs = va_response.split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            if (keyValue.length == 2) {
                keyValueMap.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }

        String vaValue = keyValueMap.get("status");
        if ("OK".equals(vaValue)) {
            updateXmlidInter(p_Uuid, p_User_Cancel);
        }

        return vaValue;
    }

    private void updateXmlidInter(String p_Uuid, String p_User_Cancel){
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

            String query = "UPDATE XMLID_INTER SET TX_USER_CANCELA = ? WHERE TX_UUID = ?";
            PreparedStatement preparedStatement = conn.prepareStatement(query);
            preparedStatement.setString(1, p_User_Cancel);
            preparedStatement.setString(2, p_Uuid);

            ResultSet resultSet = preparedStatement.executeQuery();

        } catch (SQLException e) {
            System.out.println("Error al conectarse a la base de datos: " + e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println("Error al cerrar la conexión: " + e.getMessage());
            }
        }
    }
}
