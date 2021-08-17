package common.data.spark.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataSubmitArgumentParser {
    protected final String INPUT_PATH = "-inputPath";
    protected final String OUTPUT_PATH = "-output";
    protected final String VALIDATION_PATH = "-validationpath";
    protected final String REPORT_PATH = "-report";
    protected final String META_CON_PATH = "-metaconf";
    protected final String DB_CON_PATH = "-dbconpath";
    protected final String APP_NAME = "-appname";
    protected final String JOIN_DATA_PAth = "-joinwith";
    protected final String PARTITION = "-partitions";
    protected final String HIVE_TABLE = "-htable";
    protected final String JOB_ID = "-jobid";

    //options without argument
    protected final String HELP = "-help";

    /**
     * Below is array for the canonical arguments can be passed
     * for the ETL job, along with the long name this holds the short names
     * that can be passed in the command lines
     *
     * Options not available will be handle with exception
     */
    final String[][] options = {
            {INPUT_PATH, "-i"},
            {OUTPUT_PATH, "-o"},
            {VALIDATION_PATH, "-v"},
            {REPORT_PATH, "-r"},
            {META_CON_PATH, "-c"},
            {DB_CON_PATH, "-d"},
            {APP_NAME, "-a"},
            {JOIN_DATA_PAth, "-j"},
            {PARTITION, "-p"},
            {HIVE_TABLE},
            {JOB_ID, "-j"}

    };

    final String[][] switches = {
            {HELP, "-h"}
    };


    /**
     *  Super parse method which is final, so all type parser extending this
     *  will be following this parsing method and can implement the <<handle>>
     *  method for assigning the variables values.
     *
     * @param args
     * @throws IllegalArgumentException
     */
    protected final void parse(List<String> args){
        Pattern eqSeparatedOpt = Pattern.compile("(-[^=]+)=(.+)");
        int ind = 0;
        for( ind  = 0; ind < args.size(); ind++){
            String arg = args.get(ind);
            String value = null;

            Matcher m = eqSeparatedOpt.matcher(arg);
            if(m.matches()){
                arg = m.group(1);
                value = m.group(2);
            }
            //Looking for options array with value
            String name = findInArguments(arg,options);
            if(name != null){
                if (value == null) {
                    if (ind == args.size() - 1) {
                        throw new IllegalArgumentException(
                                String.format("Missing argument for option '%s'.", arg));
                    }
                    ind++;
                    value = args.get(ind);
                }
                if(!handle(name,value)){
                    break;
                }
                continue;
            }
            name = findInArguments(arg,switches);
            if(name != null){
                if(!handle(name,null)){
                    break;
                }
                continue;
            }
            if(!handleUnknown(arg)){
                break;
            }
            if(ind < args.size()) ind ++;
            handleExtraArgs(args.subList(ind, args.size()));
        }

    }

    private String findInArguments(String arg, String[][] options) {
        for(String[] opt: options){
            for(String o: opt){
                if(arg.equals(o)){
                    return opt[0];
                }
            }
        }
        return null;
    }
    /**
     * Callback for when an option with an argument is parsed.
     *
     * @param opt The long name of the cli option (might differ from actual command line).
     * @param value The value. This will be <i>null</i> if the option does not take a value.
     * @return Whether to continue parsing the argument list.
     */
    protected boolean handle(String opt, String value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Callback for when an unrecognized option is parsed.
     *
     * @param opt Unrecognized option from the command line.
     * @return Whether to continue parsing the argument list.
     */
    protected boolean handleUnknown(String opt) {
        throw new UnsupportedOperationException();
    }

    /**
     * Callback for remaining command line arguments after either {@link #handle(String, String)} or
     * {@link #handleUnknown(String)} return "false". This will be called at the end of parsing even
     * when there are no remaining arguments.
     *
     * @param extra List of remaining arguments.
     */
    protected void handleExtraArgs(List<String> extra) {
        throw new UnsupportedOperationException();
    }
}
