package gsd.jgpaiva.controllers;

import gsd.jgpaiva.utils.GlobalConfig;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.security.InvalidParameterException;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;

public abstract class ControlImpl implements Control {
	private static final String PAR_FILENAME = "filename";
	private static final String PROTOCOL = "protocol";
	private static final String PAR_INIT_STEP = "initstep";
	private static final String PAR_FINAL_STEP = "finalstep";
	private static final String PAR_DO_EVERY = "doevery";

	private final int initStep;
	private final int finalStep;
	private final int doEvery;
	private int currentStep = -1;
	private int currentIt;
	private PrintStream psDetail = null;
	private PrintStream ps;
	protected final int pid;
	protected final String name;
	protected String filename;

	public ControlImpl(String prefix) {
		this.name = prefix;
		this.pid = Configuration.lookupPid(Configuration.getString(prefix + "."
				+ ControlImpl.PROTOCOL));
		if (Configuration.contains(prefix + "." + ControlImpl.PAR_INIT_STEP)) {
			this.initStep = Configuration.getInt(prefix + "." + ControlImpl.PAR_INIT_STEP);
			if (this.initStep < 0)
				throw new InvalidParameterException(ControlImpl.PAR_DO_EVERY
						+ " cannot be smaller than zero!");
		} else {
			this.initStep = 0;
		}
		if (Configuration.contains(prefix + "." + ControlImpl.PAR_DO_EVERY)) {
			this.doEvery = Configuration.getInt(prefix + "." + ControlImpl.PAR_DO_EVERY);
			if (this.doEvery < 1)
				throw new InvalidParameterException(ControlImpl.PAR_DO_EVERY
						+ " cannot be smaller than 1!");
		} else {
			this.doEvery = 1;
		}
		this.currentIt = this.doEvery - 1;
		if (Configuration.contains(prefix + "." + ControlImpl.PAR_FINAL_STEP)) {
			this.finalStep = Configuration.getInt(prefix + "." + ControlImpl.PAR_FINAL_STEP);
		} else {
			this.finalStep = -1;
		}

		this.filename = prefix;
		if (Configuration.contains(prefix + "." + ControlImpl.PAR_FILENAME)) {
			this.filename = Configuration.getString(prefix + "." + ControlImpl.PAR_FILENAME);
		}
		try {
			this.ps = new PrintStream(this.filename);
		} catch (FileNotFoundException e) {
			this.ps = System.out;
			System.err.println(this.name + ": could not set write to file:" + this.filename
					+ " Writing to system.out instead.");
		}
		if (GlobalConfig.getDetail()) {
			try {
				this.psDetail = new PrintStream(this.filename + ".detail");
			} catch (FileNotFoundException e) {
				this.psDetail = System.out;
				System.err.println(this.name + ": could not set write to file:" + this.filename
						+ " Writing to system.out instead.");
			}
		}
	}

	@Override
	public boolean execute() {
		this.currentStep++;
		if (this.currentStep < this.initStep) return false;

		if (this.finalStep > 0 && this.currentStep > this.finalStep) return false;
		this.currentIt++;
		if (this.currentIt == this.doEvery) {
			this.currentIt = 0;
		} else
			return false;

		return this.executeCycle();
	}

	public void println(Object arg) {
		this.ps.println(arg);
	}

	public void print(Object arg) {
		this.ps.print(arg);
	}

	public void debugPrintln(String arg) {
		if (this.psDetail != null) {
			this.psDetail.println(this.getStep() + " (" + CommonState.getTime() + ") :" + arg);
		}
	}

	public void debugPrint(String arg) {
		if (this.psDetail != null) {
			this.psDetail.print(this.getStep() + " (" + CommonState.getTime() + ") :" + arg);
		}
	}

	public int getStep() {
		return this.currentStep;
	}

	protected abstract boolean executeCycle();

	public int getFinalStep() {
		return this.finalStep;
	}
}
