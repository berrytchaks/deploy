package testflink.deploy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/git")
public class FlinkGitController {
	@Autowired
	FlinkGitService gitService;

	@RequestMapping(value="/build", method=RequestMethod.POST)
    public @ResponseBody boolean buildDeploy(@RequestBody GitParameters gitParamters){
		return gitService.build(gitParamters.getPath(),gitParamters.getRef(),gitParamters.getUrl());
	}
    
}
