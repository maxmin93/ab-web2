package net.bitnine.agensbrowser.web.message;

import org.json.simple.JSONObject;

import java.io.Serializable;

public class UploadFileDto implements Serializable {

    private static final long serialVersionUID = -1253203389607088095L;

    private String fileName;
    private String fileDownloadUri;
    private String fileType;
    private long size;

    public UploadFileDto(){
        this.fileName = "";
        this.fileDownloadUri = "";
        this.fileType = "";
        this.size = 0L;
    }
    public UploadFileDto(String fileName, String fileDownloadUri, String fileType, long size) {
        this.fileName = fileName;
        this.fileDownloadUri = fileDownloadUri;
        this.fileType = fileType;
        this.size = size;
    }

    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }

    public String getFileDownloadUri() { return fileDownloadUri; }
    public void setFileDownloadUri(String fileDownloadUri) { this.fileDownloadUri = fileDownloadUri; }

    public String getFileType() { return fileType; }
    public void setFileType(String fileType) { this.fileType = fileType; }

    public long getSize() { return size; }
    public void setSize(long size) { this.size = size; }

    @Override
    public String toString() {
        return "UploadFileDto{" +
                "fileName='" + fileName + '\'' +
                ", fileType='" + fileType + '\'' +
                '}';
    }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "file");

        json.put("name", fileName);
        json.put("type", fileType);
        json.put("size", size);
        json.put("uri", fileDownloadUri);
        return json;
    }
}
