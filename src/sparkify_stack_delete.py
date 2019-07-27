import boto3
import config
import time


# The CloudFormation client.
cloudformation = boto3.client(
    'cloudformation',
    region_name=config.AWS_REGION,
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
)

# The delay between stack status checks.
delay = 30

# A reference to the builtin function 'print()'.
builtin_print = print


def print(text):

    """
    Prints a timestamp next to the the given text.

    Args:
        text (str): The text to print.
    """

    return builtin_print('{} | {}'.format(
        time.strftime('%H:%M:%S', time.gmtime()),
        text
    ))


def get_stack_info():

    """
    Gets the description of the Sparkify stack.

    Returns:
        (dict): The description of the stack.
    """

    response = cloudformation.describe_stacks(
        StackName=config.CLOUDFORMATION_STACK_NAME
    )
    return response['Stacks'][0]


def delete_stack():

    """
    Delete the Sparkify stack.
    """

    cloudformation.delete_stack(
        StackName=config.CLOUDFORMATION_STACK_NAME
    )


def delete_sparkify_stack():

    """
    Launches the Sparkify stack deletion and waits for
    the defined resources to be also removed.
    """

    # Deletes the stack.
    print('Deleting the stack. This may take awhile, please be patient.')
    delete_stack()

    # Until the resources are removed.
    while True:

        # Gets the info corresponding the stack.
        description = get_stack_info()
        print('Current status: {}'.format(description['StackStatus']))

        # If the resources are removed.
        if description['StackStatus'] == 'DELETE_COMPLETE':
            print('Resources deleted :-)')
            break

        # Waits a few seconds until try again.
        print('Asking again in {} seconds'.format(delay))
        time.sleep(delay)


if __name__ == '__main__':
    delete_sparkify_stack()
