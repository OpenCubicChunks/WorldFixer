package io.github.opencubicchunks.worldfixer;

import static org.fusesource.jansi.Ansi.ansi;

import io.github.opencubicchunks.worldfixer.gui.GuiMain;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Color;
import org.fusesource.jansi.AnsiConsole;

import java.awt.GraphicsEnvironment;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    public static void main(String... args) throws IOException, InterruptedException {
        AnsiConsole.systemInstall();
        String ansi = System.getProperty("worldfixer.jansi", null);
        CliOutput.printChunkInfo = Boolean.parseBoolean(System.getProperty("worldfixer.printchunk", "false"));
        if (ansi == null) {
            CliOutput.jansi = !System.getProperty("os.name").startsWith("Windows") || !Ansi.isEnabled();
        } else {
            CliOutput.jansi = ansi.equalsIgnoreCase("true");
        }

        if (!GraphicsEnvironment.isHeadless()) {
            GuiMain.main();
            return;
        }
        new Main().start(args);
    }

    private void start(String... args) throws IOException, InterruptedException {


        if (args.length != 0 && (args[0].equals("-h") || args[0].equals("--help") || args[0].equals("-?"))) {
            printHelp();
            return;
        }

        if (args.length == 0) {
            args = new String[]{"-w", "world"};
        }

        if (args[0].equals("-w") || args[0].equals("--world")) {
            if (args.length != 2) {
                System.out.println(ansi().fg(Color.RED).a("Expected 2 options but got " + args.length).reset());
                printHelp();
                return;
            }
            Path path = Paths.get(args[1]);
            new WorldFixer().fixWorld(args[1], path.resolveSibling(path.getFileName() + "_fixed").toString(), new CliOutput());
        } else {
            System.out.println("Unrecognized options:\n\t" + String.join("\n\t", args) + "\n");
            printHelp();
        }
    }

    private void printHelp() {
        System.out.println("Usage: java -jar filename.jar options");
        System.out.println("available options are:\n" +
            "   -h --help -?        print this help text\n" +
            "   -w --world file     specify world location (required)");
    }

}
