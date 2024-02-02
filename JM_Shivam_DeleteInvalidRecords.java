import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.sap.aii.mapping.api.AbstractTrace;
import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;

public class JM_Shivam_DeleteInvalidRecords extends AbstractTransformation {
	int flag_reject = 0; // 0 if not rejected
	public static AbstractTrace trace;
	static boolean pipo = true;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		pipo = false;
		try {
			FileInputStream fin = new FileInputStream("Input.txt");
			FileOutputStream fout = new FileOutputStream("Output.txt");

			JM_Shivam_DeleteInvalidRecords obj = new JM_Shivam_DeleteInvalidRecords();
			obj.execute(fin, fout);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void execute(InputStream i, OutputStream o) throws StreamTransformationException {
		try {
			if(pipo)trace.addInfo("Start of execute()" + " -Shivam");
			int iteration = 0;
			List<String> lines = IOUtils.readLines(i, "UTF-8");

			for (String line : lines) {
				++iteration;
				if (iteration == 1) {
					o.write((line + "\n").getBytes());
					continue;// skipping the Headerline from validation
				}

				if ((line.trim().length() != 36 && line.trim().length() != 0)|| (line.trim().chars().filter(ch -> ch == ' ').count() != 6 && line.trim().length() != 0)) {
					if(pipo)trace.addDebugMessage("Record No: " + iteration + " was deleted because of size: "
							+ line.trim().length() + " -Shivam");
					continue;
				}
				o.write((line + "\n").getBytes());
			}

			if(pipo)trace.addInfo("End of execute()" + " -Shivam");
		} catch (Exception e) {
			// TODO: handle exception
			if(pipo)trace.addWarning(e.toString() + "Exception From execute()" + " -Shivam");
			throw new StreamTransformationException(e.toString());
		}

	}

	@Override
	public void transform(TransformationInput arg0, TransformationOutput arg1) throws StreamTransformationException {
		// TODO Auto-generated method stub
		trace = (AbstractTrace) getTrace();

		try {
			if(pipo)trace.addInfo("Start of transform()" + " -Shivam");

			execute(arg0.getInputPayload().getInputStream(), arg1.getOutputPayload().getOutputStream());

			if(pipo)trace.addInfo("End of transform()" + " -Shivam");

		} catch (Exception e) {
			// TODO: handle exception
			if(pipo)trace.addWarning(e.toString() + "Exception From transform()" + " -Shivam");
			throw new StreamTransformationException(e.toString());
		}

	}

}
