package testflink.deploy;

public interface FlinkGitService {

	boolean build(String path, String ref, String url);
}
