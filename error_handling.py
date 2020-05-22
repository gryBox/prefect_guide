from prefect.engine.state import Success, Failed, Skipped
from prefect.engine import signals


def error_notifcation_handler(obj, old_state, new_state):
    # Hamdle an empty dataframe to return a fail message.  
    # The result of a succesfull 
    if new_state.is_failed():
        p = PrefectSecret("system_errors") 
        slack_web_hook_url = p.run()

        msg = f"Task'{obj.name}' finished in state {new_state.message}"
        
        # Replace URL with your Slack webhook URL
        requests.post(slack_web_hook_url, json={"text": msg})

        # The order matters
        return_state = new_state  

        raise signals.SKIP(message='See Error Msg')

    else:
        return_state = new_state   
        
    return return_state

def imb_handler(obj, old_state, new_state):
    # Hamdle an empty dataframe to return a fail message.  
    # The result of a succesfull 
    if isinstance(new_state, Success) and new_state.result.empty:
        return_state = Failed(
            message=f"No tsx imbalance data: No trading or Data was not published to {new_state.cached_inputs['url']}", 
            result=new_state.result
            )
    else:
        return_state = new_state   

    return return_state
