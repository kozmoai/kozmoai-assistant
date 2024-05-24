# kozmoai-assistant

[![GitHub Repo stars](https://img.shields.io/github/stars/kozmoai/kozmoai-assistant?style=social)](https://github.com/kozmoai/kozmoai-assistant)
[![Discord Follow](https://dcbadge.vercel.app/api/server/8tcDQ89Ej2?style=flat)](https://discord.gg/8tcDQ89Ej2)
[![License](https://img.shields.io/github/license/kozmoai/kozmoai-assistant)](https://github.com/kozmoai/kozmoai-assistant/blob/main/LICENSE)
[![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/kozmoai/kozmoai-assistant)](https://github.com/kozmoai/kozmoai-assistant/issues)
![GitHub Release](https://img.shields.io/github/v/release/kozmoai/kozmoai-assistant)
[![Twitter Follow](https://img.shields.io/twitter/follow/antonosika?style=social)](https://twitter.com/antonosika)

kozmoai-assistant lets you:
- Specify software in natural language
- Sit back and watch as an AI writes and executes the code
- Ask the AI to implement improvements

## Getting Started

### Install kozmoai-assistant

For **stable** release:

- `python -m pip install kozmoai-assistant`

For **development**:
- `git clone https://github.com/kozmoai/kozmoai-assistant.git`
- `cd kozmoai-assistant`
- `poetry install`
- `poetry shell` to activate the virtual environment

We actively support Python 3.10 - 3.12. The last version to support Python 3.8 - 3.9 was [0.2.6](https://pypi.org/project/kozmoai-assistant/0.2.6/).

### Setup API Key

Choose **one** of:
- Export env variable (you can add this to .bashrc so that you don't have to do it each time you start the terminal)
    - `export OPENAI_API_KEY=[your api key]`
- .env file:
    - Create a copy of `.env.template` named `.env`
    - Add your OPENAI_API_KEY in .env
- Custom model:
    - See [docs](https://kozmoai-assistant.readthedocs.io/en/latest/open_models.html), supports local model, azure, etc.

Check the [Windows README](./WINDOWS_README.md) for Windows usage.

**Other ways to run:**
- Use Docker ([instructions](docker/README.md))
- Do everything in your browser:
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/kozmoai/kozmoai-assistant/codespaces)

### Create new code (default usage)
- Create an empty folder for your project anywhere on your computer
- Create a file called `prompt` (no extension) inside your new folder and fill it with instructions
- Run `gpte <project_dir>` with a relative path to your folder
  - For example: `gpte projects/my-new-project` from the kozmoai-assistant directory root with your new folder in `projects/`

### Improve Existing Code
- Locate a folder with code which you want to improve anywhere on your computer
- Create a file called `prompt` (no extension) inside your new folder and fill it with instructions for how you want to improve the code
- Run `gpte <project_dir> -i` with a relative path to your folder
  - For example: `gpte projects/my-old-project -i` from the kozmoai-assistant directory root with your folder in `projects/`

By running kozmoai-assistant you agree to our [terms](https://github.com/kozmoai/kozmoai-assistant/blob/main/TERMS_OF_USE.md).


## Features

### Pre Prompts
You can specify the "identity" of the AI agent by overriding the `preprompts` folder, with your own version of the `preprompts`, using the `--use-custom-preprompts` argument.

Editing the `preprompts` is how you make the agent remember things between projects.

### Vision

By default, kozmoai-assistant expects text input via a `prompt` file. It can also accept imagine inputs for vision-capable models. This can be useful for adding UX or architecture diagrams as additional context for KOZMOAI Assistant. You can do this by specifying an image directory with the—-image_directory flag and setting a vision-capable model in the second cli argument.

E.g. `gpte projects/example-vision gpt-4-vision-preview --prompt_file prompt/text --image_directory prompt/images -i`

### Open source, local and alternative models

By default, kozmoai-assistant supports OpenAI Models via the OpenAI API or Azure Open AI API, and Anthropic models.

With a little extra set up you can also run with open source models, like WizardCoder. See the [documentation](https://kozmoai-assistant.readthedocs.io/en/latest/open_models.html) for example instructions.

## Mission

The kozmoai-assistant community mission is to **maintain tools that coding agent builders can use and facilitate collaboration in the open source community**.

If you are interested in contributing to this, we are interested in having you.

If you want to see our broader ambitions, check out the [roadmap](https://github.com/kozmoai/kozmoai-assistant/blob/main/ROADMAP.md), and join
[discord](https://discord.gg/8tcDQ89Ej2)
to get input on how you can [contribute](.github/CONTRIBUTING.md) to it.

kozmoai-assistant is [governed](https://github.com/kozmoai/kozmoai-assistant/blob/main/GOVERNANCE.md) by a board of long-term contributors. If you contribute routinely and have an interest in shaping the future of kozmoai-assistant, you will be considered for the board.

## Example



https://github.com/kozmoai/kozmoai-assistant/assets/4467025/40d0a9a8-82d0-4432-9376-136df0d57c99
