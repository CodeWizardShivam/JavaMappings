import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.sap.aii.mapping.api.AbstractTrace;
import com.sap.aii.mapping.api.AbstractTransformation;
import com.sap.aii.mapping.api.DynamicConfiguration;
import com.sap.aii.mapping.api.DynamicConfigurationKey;
import com.sap.aii.mapping.api.StreamTransformationException;
import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.TransformationOutput;
import com.sun.xml.internal.ws.util.StringUtils;

public class JM_NoOfColNRecLenNFileSizeValidation extends AbstractTransformation {
	int flag_reject = 0; // 0 if not rejected
	public static AbstractTrace trace;
	static boolean pipo = true;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		pipo = false;

		try {
			FileInputStream fin = new FileInputStream("Input.txt");
			FileOutputStream fout = new FileOutputStream("Output.txt");

			JM_NoOfColNRecLenNFileSizeValidation obj = new JM_NoOfColNRecLenNFileSizeValidation();
			obj.execute(fin, fout);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void execute(InputStream i, OutputStream o) throws StreamTransformationException {
		try {
			if(pipo)trace.addInfo("Start of execute()" + " -Shivam");
			int iteration = 1;
			List<String> lines = IOUtils.readLines(i, "UTF-8");

			for (String line : lines) {
				++iteration;
				
				if ((line.trim().length() != 36 && line.trim().length() != 0) || (line.trim().chars().filter(ch -> ch == ' ').count() != 6 && line.trim().length() != 0)) {
					flag_reject = 2;

					if(pipo)trace.addDebugMessage("Rejected Because of Record: " + iteration + " -Shivam");
					break;
				}
			}
//			o.write(("flag_reject:"+flag_reject+ "\n").getBytes());

			o.write(("MC MCNO PUNCHDT PUNCHTIME RFID IND INOUTIND" + "\n").getBytes());
			for (String line : lines) {
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

			DynamicConfiguration dc = arg0.getDynamicConfiguration();

			DynamicConfigurationKey key_path = DynamicConfigurationKey.create("http://sap.com/xi/XI/System/File",
					"Directory");

			DynamicConfigurationKey key_size = DynamicConfigurationKey.create("http://sap.com/xi/XI/System/File",
					"SourceFileSize");

			if(pipo)trace.addDebugMessage("File Size: " + dc.get(key_size) + " -Shivam");
			if (dc.get(key_size) != null) {
				int key_size_int = Integer.parseInt(dc.get(key_size));
				if (key_size_int > 2097152) {
					flag_reject = 1;
					if(pipo)trace.addDebugMessage("Rejected Because of Size: " + dc.get(key_size) + " -Shivam");
				}
			}

			String finalFilePath;
			if (flag_reject == 0)
				finalFilePath = "/xidata/hr_timedata/ps_RFID";
			else if (flag_reject == 1) //Size > 2MB
				finalFilePath = "/xidata/hr_timedata/ps_reject/ps_size";
			else
				finalFilePath = "/xidata/hr_timedata/ps_reject";
			
			if(pipo)trace.addDebugMessage("Target Path: " + finalFilePath + " -Shivam");
			dc.put(key_path, finalFilePath);
			if(pipo)trace.addInfo("End of transform()" + " -Shivam");

		} catch (Exception e) {
			// TODO: handle exception
			if(pipo)trace.addWarning(e.toString() + "Exception From transform()" + " -Shivam");
			throw new StreamTransformationException(e.toString());
		}

	}

}
