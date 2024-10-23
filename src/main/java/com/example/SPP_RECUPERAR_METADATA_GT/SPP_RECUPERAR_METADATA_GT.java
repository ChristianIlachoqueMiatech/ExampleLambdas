package com.example.SPP_RECUPERAR_METADATA_GT;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.json.JSONObject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

public class SPP_RECUPERAR_METADATA_GT implements RequestHandler<Input, String> {
    @Override
    public String handleRequest(Input input, Context context) {
        String p_Fecha = input.getP_Fecha();
        String p_Tipo = input.getP_Tipo();

        return callLaFeTimbrarboletoRecuperar(p_Fecha, p_Tipo);
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

    public String callLaFeTimbrarboletoRecuperar(String p_Fecha, String p_Tipo){
        String va_response = "";
        Region region = Region.EU_WEST_2;
        LambdaClient awsLambda = LambdaClient.builder()
                .region(region)
                .build();

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("FECHA", p_Fecha);
        jsonObj.put("TIPO", p_Tipo);
        String payload = jsonObj.toString();

        va_response = invokeLambda(awsLambda, "arn:aws:lambda:us-east-1:119260075650:function:la_fe_timbrarboleto_recuperar_metadata_pac_gt", payload)
                .replace("\"", "")
                .replace("\n", "")
                .replace("{", "")
                .replace("}", "");

        return va_response;
    }
}
