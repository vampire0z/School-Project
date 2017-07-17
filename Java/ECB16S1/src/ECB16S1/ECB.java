package ECB16S1;

public class ECB {

	public static void main(String[] args) {
		//run the whole program
		Read r = new Read(args);
		//read the contact list
		r.readPhoneSample();
		//read the instruction list
		r.readInstructionSample();
	}

}
