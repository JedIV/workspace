By default, you will find here all coding objects that are synchronized with Dataiku DSS.
A full description is available from the doc: https://doc.dataiku.com/dss/latest/source/code-studios/concepts.rst

Here is the description of the default folders, if your administrator chose to synchronize them, and did not change their name or location:
code_studio-versioned: 
    Files specific to this Code Studio. 
    Synchronized to a versioned folder on DSS (accessible through "Files > Versioned" in a Code Studio). 
    Useful to store code, settings, etc...
code_studio-resources: 
    Files specific to this Code Studio. 
    Synchronized to a non-versioned folder on DSS (accessible through "Files > Resources" in a Code Studio). 
    Useful to store static files or artefacts such as images or models.
project-lib-versioned: 
    Content of the project libraries.
    (accessible through "Libraries > Libraries" in a project)
project-lib-resources: 
    Shared resources for this project, stored next to the libraries. Contrary to standard libraries files, these are not versioned and are accessible through "Libraries > Resources" in a project (menu on the top right corner).
    Useful for artefacts used by a whole project (icon, images, etc...)
user-versioned: 
    Files specific to a user, synchronized to all his Code Studios.
    Synchronized to a versioned folder on DSS (accessible through "My files > User config" in each user's profiles).
    Useful to store user settings. This is used specifically for Dataiku built-in Code Studio templates to persist your configuration (for example, JupyterLab settings).
user-resources: 
    Files specific to a user, synchronized to all his Code Studios.
    Synchronized to a non-versioned folder on DSS (accessible through "My files > User resources" in each user's profiles).
    Useful to store artefact-like user settings, like plugins and tools.
recipes: 
    Contains all the coding recipes of the project.
    Note that you can only edit recipes, creating a file in this folder will not create a recipe in your flow.
