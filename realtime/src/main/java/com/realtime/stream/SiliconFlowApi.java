package com.realtime.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.stream.common.utils.ConfigUtils;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SiliconFlowApi {

    //  静态块：禁用 FastJSON 的循环引用检测
    static {
        JSONObject.DEFAULT_GENERATE_FEATURE |= SerializerFeature.DisableCircularReferenceDetect.getMask();
    }

    // 设置 OkHttp 连接池，最多保持 200 个连接，超时时间为 5 分钟
    private static final ConnectionPool CONNECTION_POOL = new ConnectionPool(200, 5, TimeUnit.MINUTES);
    // 指定调用的大模型 API 地址
    private static final String SILICON_API_ADDR = "https://api.siliconflow.cn/v1/chat/completions";
    // 从配置文件中读取访问令牌
    private static final String SILICON_API_TOKEN = ConfigUtils.getString("silicon.api.token");
    // 构建 OkHttp 客户端，设置连接池、超时时间等参数
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .connectionPool(CONNECTION_POOL)
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build();

    //核心方法：generateBadReview,向指定的 API 发送请求，生成一条“差评”文本。
    // 它接收两个参数：
    // prompt：用户提供的提示信息，用于指导生成的内容。
    // apiToken：用于身份验证的 API 令牌。
    public static String generateBadReview(String prompt, String apiToken) {
        try {
            //调用 buildRequestBody 方法，使用提供的 prompt 构建 JSON 格式的请求体，包含模型信息和生成参数
            JSONObject requestBody = buildRequestBody(prompt);
            //线程休眠 1 毫秒，可能用于节流或防止请求过于频繁。
            Thread.sleep(1);
            //构建 HTTP 请求
            //URL：请求发送到 SILICON_API_ADDR 指定的地址。
            Request request = new Request.Builder()
                    .url(SILICON_API_ADDR)
                    //请求体：将构建的 JSON 请求体转换为字符串，并设置媒体类型为 application/json。
                    .post(
                            RequestBody.create(
                            MediaType.parse("application/json; charset=utf-8"),
                            requestBody.toJSONString()
                            )
                    )
                    //头信息：
                    //Authorization：使用 Bearer 令牌进行身份验证。
                    //Content-Type：指定请求体的媒体类型为 JSON。
                    .addHeader("Authorization", "Bearer " + apiToken)
                    .addHeader("Content-Type", "application/json")
                    .build();

            //使用 OkHttp 客户端发送同步请求。
            try (Response response = CLIENT.newCall(request).execute()) {
                //如果响应不成功（状态码非 2xx），调用 handleError 方法处理错误，并返回错误信息。
                if (!response.isSuccessful()) {
                    handleError(response);
                    return "请求失败: HTTP " + response.code();
                }
                //如果响应成功，调用 processResponse 方法解析响应内容，提取生成的文本。
                return processResponse(response);
            }
        } catch (IOException e) {
            handleException(e);
            return "网络异常: " + e.getMessage();
        } catch (Exception e) {
            return "系统错误: " + e.getMessage();
        }
    }

    //私有方法：buildRequestBody
    private static JSONObject buildRequestBody(String prompt) {
        return new JSONObject()
                // 设置使用的大模型名称
                .fluentPut("model", "Pro/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B")
                // 不启用流式输出
                .fluentPut("stream", false)
                // 最多生成 512 个 token
                .fluentPut("max_tokens", 512)
                // 控制生成文本的随机性
                .fluentPut("temperature", 0.7)
                // 控制采样概率范围
                .fluentPut("top_p", 0.7)
                // 控制采样数量
                .fluentPut("top_k", 50)
                // 减少重复内容
                .fluentPut("frequency_penalty", 0.5)
                // 生成一个结果
                .fluentPut("n", 1)
                .fluentPut("messages", new JSONObject[]{
                        new JSONObject()
                                // 用户角色
                                .fluentPut("role", "user")
                                // 提示词内容
                                .fluentPut("content", prompt)
                });
    }

    //私有方法：processResponse
    private static String processResponse(Response response) throws IOException {
        //响应体为空则抛出异常
        if (response.body() == null) {
            throw new IOException("空响应体");
        }

        //获取响应字符串
        String responseBody = response.body().string();
        //解析为 JSON 对象
        JSONObject result = JSON.parseObject(responseBody);

        if (!result.containsKey("choices")) {
            throw new RuntimeException("无效响应结构: 缺少choices字段");
        }

        if (result.getJSONArray("choices").isEmpty()) {
            throw new RuntimeException("响应内容为空");
        }

        // 提取最终生成的内容
        return result.getJSONArray("choices")
                .getJSONObject(0)
                .getJSONObject("message")
                .getString("content");
    }

    //获取错误详情
    private static void handleError(Response response) throws IOException {
        String errorBody = response.body() != null ?
                response.body().string() : "无错误详情";
        System.err.println("API错误 [" + response.code() + "]: " + errorBody);
    }

    private static void handleException(IOException e) {
        System.err.println("网络异常: " + e.getMessage());
        e.printStackTrace();
    }

    public static void main(String[] args) {
        // 测试用例（需要有效token）
        String result = generateBadReview(
                "给出一个电商差评，攻击性拉满，使用脏话，20字数以内，不需要思考过程",
                // 使用配置中的 Token
                SILICON_API_TOKEN
        );
        System.out.println("生成结果: " + result);
    }
}