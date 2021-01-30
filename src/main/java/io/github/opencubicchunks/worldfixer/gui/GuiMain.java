package io.github.opencubicchunks.worldfixer.gui;

import io.github.opencubicchunks.worldfixer.CliOutput;
import io.github.opencubicchunks.worldfixer.StatusHandler;
import io.github.opencubicchunks.worldfixer.Utils;
import io.github.opencubicchunks.worldfixer.WorldFixer;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.WindowConstants;
import javax.swing.border.EmptyBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

public class GuiMain extends JFrame {

    public static void main(String... args) {
        EventQueue.invokeLater(() -> {
            new GuiMain().start();
        });
    }

    private boolean hasChangedDst;

    private JLabel statusLabel;
    private JLabel chunkStatusLabel;
    private JButton beginButton;
    private JProgressBar progressBar;

    private JTextField srcPathField;
    private JTextField dstPathField;
    private boolean isFixing;

    public GuiMain() {
        init();
    }

    private void start() {
        this.setVisible(true);
    }

    private void init() {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException e) {
            throw new Error("System Look and Feel shouldn't throw exception", e);
        }
        this.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        this.setMinimumSize(new Dimension(640, 0));

        JPanel root = new JPanel(new BorderLayout());

        JPanel mainPanel = new JPanel(new GridBagLayout());

        JPanel selection = new JPanel(new GridBagLayout());
        addSelectFilePanel(selection, false);
        addSelectFilePanel(selection, true);

        beginButton = new JButton("Fix");
        progressBar = new JProgressBar();
        // hack to make the layout not remove their space
        statusLabel = new JLabel(" ");
        chunkStatusLabel = new JLabel(" ");

        GridBagConstraints gbc = new GridBagConstraints();

        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 2;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        mainPanel.add(selection, gbc);

        gbc.gridx = 1;
        gbc.gridy = 3;
        gbc.gridwidth = 1;
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.EAST;
        mainPanel.add(beginButton, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.gridwidth = 1;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        mainPanel.add(statusLabel, gbc);

        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.gridwidth = 1;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        mainPanel.add(chunkStatusLabel, gbc);

        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.gridwidth = 1;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        mainPanel.add(progressBar, gbc);

        root.add(mainPanel, BorderLayout.CENTER);
        root.setBorder(new EmptyBorder(10, 10, 10, 10));

        updateFixBtn();
        beginButton.addActionListener(x -> convert());

        progressBar.setPreferredSize(new Dimension(100, (int) beginButton.getPreferredSize().getHeight()));

        this.add(root);

        this.pack();
        this.setMinimumSize(new Dimension(200, this.getHeight()));
        this.setTitle("CubicChunks WorldFixer");
    }


    private void addSelectFilePanel(JPanel panel, boolean isSrc) {
        JLabel label = new JLabel(isSrc ? "Source: " : "Destination: ");

        Path srcPath = Utils.getApplicationDirectory().resolve("saves").resolve("New World");
        Path dstPath = getDstForSrc(srcPath);
        JTextField path = new JTextField((isSrc ? srcPath : dstPath).toString());
        if (isSrc) {
            this.srcPathField = path;
        } else {
            this.dstPathField = path;
        }
        JButton selectBtn = new JButton("...");

        GridBagConstraints gbc = new GridBagConstraints();

        gbc.gridy = isSrc ? 0 : 1;

        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 0;
        gbc.weightx = 0;
        panel.add(label, gbc);

        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 1;
        gbc.weightx = 1;
        panel.add(path, gbc);

        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 2;
        gbc.weightx = 0;
        panel.add(selectBtn, gbc);

        selectBtn.addActionListener(e -> {
            JFileChooser chooser = new JFileChooser();
            chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            chooser.setDialogType(JFileChooser.CUSTOM_DIALOG);
            chooser.setMultiSelectionEnabled(false);
            chooser.setFileHidingEnabled(false);
            chooser.setCurrentDirectory(getDefaultSaveLocation().toFile());
            int result = chooser.showDialog(this, "Select");
            if (result == JFileChooser.APPROVE_OPTION) {
                onSelectLocation(isSrc, chooser);
            }
        });
        selectBtn.setPreferredSize(new Dimension(30, (int) path.getPreferredSize().getHeight()));
        selectBtn.setMinimumSize(new Dimension(30, (int) path.getPreferredSize().getHeight()));

        path.getDocument().addDocumentListener(new DocumentListener() {

            @Override public void insertUpdate(DocumentEvent e) {
                update();
            }

            @Override public void removeUpdate(DocumentEvent e) {
                update();
            }

            @Override public void changedUpdate(DocumentEvent e) {
                update();
            }

            private void update() {
                if (!isSrc) {
                    hasChangedDst = true;
                }
                updateFixBtn();
            }
        });


        label.setHorizontalAlignment(SwingConstants.RIGHT);
    }


    private void updateFixBtn() {
        beginButton.setEnabled(!isFixing && Utils.isValidPath(dstPathField.getText()) && Utils.fileExists(srcPathField.getText()));
    }

    private void onSelectLocation(boolean isSrc, JFileChooser chooser) {
        Path file = chooser.getSelectedFile().toPath();

        if (isSrc) {
            srcPathField.setText(file.toString());
            if (!hasChangedDst) {
                dstPathField.setText(getDstForSrc(file).toString());
            }
        } else {
            dstPathField.setText(file.toString());
            hasChangedDst = true;
        }
        updateFixBtn();
    }


    private Path getDstForSrc(Path src) {
        return src.getParent().resolve(src.getFileName().toString() + "_FIXED");
    }

    private Path getDefaultSaveLocation() {
        return Utils.getApplicationDirectory().resolve("saves");
    }

    private void convert() {
        if (!Utils.fileExists(srcPathField.getText())) {
            updateFixBtn();
            return;
        }
        Path srcPath = Paths.get(srcPathField.getText());
        Path dstPath;
        try {
            dstPath = Paths.get(dstPathField.getText());
        } catch (InvalidPathException e) {
            updateFixBtn();
            return;
        }
        if (Files.exists(dstPath) && !Files.isDirectory(dstPath)) {
            JOptionPane.showMessageDialog(this, "The destination is not a directory!", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }
        try {
            if (!Utils.isEmpty(dstPath)) {
                String[] options = {"Cancel", "Continue"};
                int result = JOptionPane.showOptionDialog(this, "The selected destination directory is not empty.\nThis may result in overwriting "
                        + "or losing all data in this directory!\n\nDo you want cancel and select another directory?",
                    "Warning", JOptionPane.DEFAULT_OPTION, JOptionPane.WARNING_MESSAGE, null, options, "Cancel");
                if (result == JOptionPane.CLOSED_OPTION) {
                    return; // assume cancel
                }
                if (result == 0) {
                    return;
                }
            }
        } catch (IOException e) {
            JOptionPane.showMessageDialog(this, "Error while checking if destination directory is empty!", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }
        progressBar.setStringPainted(true);
        isFixing = true;
        updateFixBtn();

        Thread thread = new Thread(() -> {
            try {
                new WorldFixer().fixWorld(srcPath.toString(), dstPath.toString(), new GuiStatusHandler());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread.setName("fixer-thread");
        thread.setDaemon(true);
        thread.start();
    }

    private class GuiStatusHandler implements StatusHandler {

        private final CliOutput cli = new CliOutput();

        private static final long UPDATE_DELTA = 200;
        private long lastUpdate = 0;
        private volatile double lastProgressValue;
        private volatile String lastProgressString;
        private volatile String lastInfo = " ";
        private volatile String lastChunkInfo = " ";

        @Override public void status(String txt) {
            cli.status(txt);
        }

        @Override public void info(String txt) {
            cli.info(txt);
            this.lastInfo = txt.isEmpty() ? " " : txt;
            if (System.currentTimeMillis() - lastUpdate > UPDATE_DELTA) {
                lastUpdate = System.currentTimeMillis();
                EventQueue.invokeLater(this::updateProgress);
            }
        }

        @Override public void chunkInfo(Supplier<String> info) {
            cli.chunkInfo(info);
            String lastChunkInfo = info.get();
            this.lastChunkInfo = lastChunkInfo.isEmpty() ? " " : lastChunkInfo;
            if (System.currentTimeMillis() - lastUpdate > UPDATE_DELTA) {
                lastUpdate = System.currentTimeMillis();
                EventQueue.invokeLater(this::updateProgress);
            }
        }

        @Override public void progress(Supplier<Double> progressValue, Supplier<String> progressString, boolean isDone) {
            cli.progress(progressValue, progressString, isDone);
            this.lastProgressValue = progressValue.get();
            this.lastProgressString = progressString.get();
            if (isDone) {
                EventQueue.invokeLater(this::updateProgressDone);
                return;
            }
            if (System.currentTimeMillis() - lastUpdate > UPDATE_DELTA) {
                lastUpdate = System.currentTimeMillis();
                EventQueue.invokeLater(this::updateProgress);
            }
        }

        private void updateProgress() {
            progressBar.setString(lastProgressString);
            progressBar.setValue((int) (lastProgressValue * 100));
            statusLabel.setText(lastInfo);
            chunkStatusLabel.setText(lastChunkInfo);
        }

        private void updateProgressDone() {
            isFixing = false;
            progressBar.setString(lastProgressString);
            progressBar.setValue(0);
            statusLabel.setText("DONE");
            chunkStatusLabel.setText(" ");
            updateFixBtn();
        }

        @Override public void error(String msg, Throwable exception) {
            cli.error(msg, exception);
        }

        @Override public void warning(String msg) {
            cli.warning(msg);
        }
    }
}