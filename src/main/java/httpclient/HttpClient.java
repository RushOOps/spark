//package httpclient;
//
//import org.apache.http.HttpResponse;
//import org.apache.http.HttpStatus;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClientBuilder;
//import org.apache.http.util.EntityUtils;
//
//import java.io.IOException;
//
//public class HttpClient {
//
//    public final static String FENCI = "http://hwtest.semantic-nlp.chiq-cloud.com/NlpBasicTasks?userFlag=0&words=";
//    public final static String GRAMMAR = "http://hwtest.dev-semantic.chiq-cloud.com/strong?test=1&userid=1&text=";
//    public final static int TIMEOUT = 60000;
//
//    /**
//     * 以get方式调用第三方接口
//     * @param url
//     * @return
//     */
//    public static String doGet(String url) throws IOException {
//
//        RequestConfig requestConfig = RequestConfig.custom()
//                .setConnectTimeout(TIMEOUT)
//                .setConnectionRequestTimeout(TIMEOUT)
//                .setSocketTimeout(TIMEOUT)
//                .build();
//
//        //创建HttpClient对象
//        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
//        HttpGet httpGet = new HttpGet(url);
//        httpGet.setConfig(requestConfig);
//
//        try {
//            httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36");
//            HttpResponse response = httpClient.execute(httpGet);
//            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
//                //返回json格式
//                return EntityUtils.toString(response.getEntity());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }finally {
//            httpClient.close();
//        }
//        return null;
//    }
//
//}
