
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.Map;
import java.util.stream.Collectors;
import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class Output {
    //all inputs needed to generate output file
    String path="";
    String output="";
    private LinkedHashMap<String, Double> sortedPages;
    private HashMap<String,Integer> inLinks;
    private HashMap<String,Integer> outLinks;

    //empty constructor
    public Output(){

    }

    /**
     *
     * @param path location of hadoop last generated file
     * @return object ref
     */
    public Output setPath(String path){
        this.path = path;
        return this;
    }

    /**
     *
     * @param inLinks LinkedHashMap has Key = page name
     * @return object ref
     */
    public Output setInLinks(HashMap<String,Integer> inLinks){
        this.inLinks = inLinks;
        return this;
    }

    /**
     *
     * @param outLinks LinkedHashMap has Key = page name
     * @return Object ref
     */
    public Output setOutLinks(HashMap<String,Integer> outLinks){
        this.outLinks = outLinks;
        return this;
    }

    /**
     *
     * @param output output file location //should be dir
     * @return Object ref
     */
    public Output setOutputDir(String output) {
        this.output = output+ FileSystems.getDefault().getSeparator() +"output.txt";
        return this;
    }

    /**
     * sorts the output file based as page rank
     * @return Object Ref
     */
    public Output sort() {
        this.sortedPages = convertFileToMap();
        return this;
    }

    public Output filter(int rows) {
        if(this.sortedPages.size()>rows){
            this.sortedPages = sortedPages.entrySet().stream()
                    .limit(rows)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
        }
        return this;
    }

    /**
     * generates output file
     */
    public void generateOutput(){
        try {
            File myObj = new File(output);
            FileWriter myWriter = new FileWriter(myObj);
            myWriter.write("Page Name\tPage Rank\tout links\tin links\n");
            myWriter.write("__________________________________________\n\n");
            for (Map.Entry<String,Double> entry:sortedPages.entrySet()) {
                myWriter.write(entry.getKey()+"\t"+entry.getValue()+"\t"+outLinks.get(entry.getKey())+"\t"+inLinks.get(entry.getKey()));
                myWriter.write("\n");
            }
            myWriter.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            deleteDirs();
        }
    }

    /**
     * delete all files created by hadoop
     */
    private void deleteDirs() {
        try {
            Path rootDir = new File(output).toPath().getParent();
            final List<Path> pathsToDelete = Files.walk(rootDir).sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path path : pathsToDelete) {
                if(!path.toString().equals(output) && !path.toString().equals(rootDir.toString())) {
                    Files.deleteIfExists(path);
                }
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }

    /**
     * converts inputfile into a hashmap to sort them based on pagerank
     * @return LinkedHashMap with sorted paged in desc based on page rank
     */
    private LinkedHashMap<String, Double> convertFileToMap() {
        LinkedHashMap<String, Double> pages =new LinkedHashMap<>();
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String pageName = data.split("\t")[0];
                String rank = data.split("\t")[1].split(",")[0];
                pages.put(pageName,Double.parseDouble(rank)+1); // add + 1 to rank given
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        //sort the pages based on rank
        return pages.entrySet().stream().sorted(comparingByValue(Comparator.reverseOrder()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }
}
