import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Util {
    private final HashMap<String,Integer> inLinksCount = new HashMap<>();
    private final HashMap<String,Integer> outLinksCount = new HashMap<>();

    public Util generateInAndOutLinks(String path){
        try{
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;

            while ((strLine = br.readLine()) != null) {

                String[] outlinks = strLine.split("\t")[1].split(",");
                outLinksCount.put(strLine.split("\t")[0],outlinks.length);

                for (String pageNum: outlinks) {
                    if (!inLinksCount.containsKey(pageNum)){
                        inLinksCount.put(pageNum , 1);
                    }else{
                        inLinksCount.put(pageNum , inLinksCount.get(pageNum)+1);
                    }
                }
            }
        }catch (Exception e){
            System.out.println(e);
        }
        return this;
    }

    public HashMap<String, Integer> getInLinks(){
        return inLinksCount;
    }

    public HashMap<String, Integer> getOutLinks(){
        return outLinksCount;
    }
}
