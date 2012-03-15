package com.github.cwilper.fcrepo.store.util;

import com.github.cwilper.fcrepo.store.core.StoreException;
import com.github.cwilper.fcrepo.store.util.commands.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Command-line entry point.
 */
public class StoreUtil {
    private static final String[] DEFAULT_CONFIG_LOCATIONS = new String[]
            { "beans.xml" };
    
    private static final Logger logger =
            LoggerFactory.getLogger(StoreUtil.class);
   
    private final String[] configLocations;

    public StoreUtil(String[] configLocations) {
        this.configLocations = configLocations;
    }
    
    public void execute(String[] args) throws StoreException {
        if (args.length == 0)
            throw new IllegalArgumentException("At least one arg required");
        for (int i = 1; i < args.length; i++) {
            System.setProperty("arg" + i, args[i]);
        }
        for (int i = args.length; i < 20; i++) {
            System.setProperty("arg" + i, "required-argument-missing");
        }

        ClassPathXmlApplicationContext context =
                new ClassPathXmlApplicationContext(configLocations);
        try {
            Command command = context.getBean(args[0], Command.class);
            command.execute();
        } finally {
            context.close();
        }
    }

    public static void main(String[] args) {
        try {
            new StoreUtil(DEFAULT_CONFIG_LOCATIONS).execute(args);
        } catch (Throwable th) {
            String message = th.getMessage();
            if (message.contains("required-argument-missing")) {
                message = "Required argument missing for command";
            } else if (message.startsWith("No bean named '" + args[0] + "'")) {
                message = "No such command: " + args[0];
            } else if (!message.contains("No bean named")) {
                logger.error("", th);
            }
            System.out.println("ERROR: " + message);
            System.exit(1);
        }
    }
}
