package tn.insat.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {

        // Vérifie les arguments
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

        // ✅ Nécessaire pour les plateformes Windows
        System.setProperty("hadoop.home.dir", "C:/hadoop");

        // ✅ Configuration Hadoop locale
        Configuration conf = new Configuration();
        conf.setBoolean("hadoop.native.lib", false); // Fix erreur NativeIO
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("hadoop.tmp.dir", "C:/tmp/hadoop-user"); // Assure-toi que ce dossier existe

        // Configuration du Job
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        // Mapper, Reducer, Combiner
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Types de sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exécution du job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
