package org.backupLW.functions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.time.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

/**
 * Azure Functions with Timer trigger.
 */
public class BackupLWConfig {
    private static final String path = "/home/lwConfigBackup/";
    private Triple fusionDev;
    private Triple fusionStg;
    private Triple fusionProd;
    private List<String> devEnvAppList;
    private List<String> stgEnvAppList;
    private List<String> prodEnvAppList;

    /**
     * This function will be invoked periodically according to the specified schedule.
     */
    @FunctionName("BackupLWConfig")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = "0 0 0 * * *") String timerInfo,
        final ExecutionContext context
    ) {
        context.getLogger().info("start to backup fusion configuration at: " + LocalDateTime.now());
        init();
        List<Pair<Triple, String>> app = new ArrayList<Pair<Triple, String>>(){{
            addAll(devEnvAppList.stream().map(appName -> Pair.of(fusionDev, appName)).collect(Collectors.toList()));
            addAll(stgEnvAppList.stream().map(appName -> Pair.of(fusionStg, appName)).collect(Collectors.toList()));
            addAll(prodEnvAppList.stream().map(appName -> Pair.of(fusionProd, appName)).collect(Collectors.toList()));
        }};
        processDownload(app, context);
    }

    private void processDownload(List<Pair<Triple, String>> appList, ExecutionContext context) {
        LocalDate date = LocalDate.now();
        ExecutorService executorService = new ThreadPoolExecutor(0, 14, 60, TimeUnit.SECONDS, new SynchronousQueue<>());
        try {
            Collection<Callable<Pair>> tasks = appList.stream().map(app -> (Callable<Pair>) () -> downloadFile(app.getLeft(), app.getRight(), date, context)).collect(Collectors.toList());
            List<Future<Pair>> futures = executorService.invokeAll(tasks);
            for (Future<Pair> future : futures) {
                Pair<File, Boolean> result = future.get();
                if (!result.getRight()) {
                    if (result.getLeft().exists()) {
                        result.getLeft().delete();
                    }
                    context.getLogger().info("Download file " + result.getLeft().getName() + " failed");
                } else {
                    context.getLogger().info("Download file " + result.getLeft().getName() + " success");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            rollBack(appList, date);
            context.getLogger().info("Backup process failed, delete all processed file");
        } finally {
            executorService.shutdown();
        }
    }

    private Pair<File, Boolean> downloadFile(Triple fusion, String appName, LocalDate date, ExecutionContext context) throws IOException {
        InputStream inputStream = null;
        File directory = new File(path);
        directory.mkdirs();
        String fileName = path + "lw" + fusion.getRight() + appName + "_" + date + ".zip";
        File file = new File(fileName);
        try {
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();
            URL url = new URL(fusion.getMiddle() + appName);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("Authorization", "Basic " + fusion.getLeft());
            conn.setReadTimeout(1000*40);
            conn.setConnectTimeout(1000*10);
            inputStream = conn.getInputStream();

            FileUtils.copyInputStreamToFile(inputStream, file);
        } catch (IOException e) {
            e.printStackTrace();
            return Pair.of(file, false);
        } finally {
            if (null != inputStream) {
                inputStream.close();
            }
        }
        return Pair.of(file, true);
    }

    private void rollBack(List<Pair<Triple, String>> appList, LocalDate date) {
        appList.forEach(app -> {
            String fileName = path + "lw" + app.getLeft().getRight() + app.getRight() + "_" + date + ".zip";
            File file = new File(fileName);
            if (file.exists()) {
                file.delete();
            }
        });
    }

    private void init() {
        fusionDev = Triple.of("credential",
                "https://ferguson-dev.b.lucidworks.cloud/api/objects/export?app.ids=",
                "dev");
        fusionStg = Triple.of("credential",
                "https://ferguson-stg.b.lucidworks.cloud/api/objects/export?app.ids=",
                "stg");
        fusionProd = Triple.of("credential",
                "https://ferguson.b.lucidworks.cloud/api/objects/export?app.ids=",
                "prod");
        devEnvAppList = new ArrayList<String>() {{
            add("ferguson");
            add("ferguson_app1");
            add("ferguson_app2");
        }};
        stgEnvAppList = new ArrayList<String>() {{
            add("ferguson");
            add("ferguson_app1");
            add("ferguson_app2");
        }};
        prodEnvAppList = new ArrayList<String>() {{
            add("ferguson");
        }};
    }
}
