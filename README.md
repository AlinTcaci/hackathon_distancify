The Application solves the problems 1-2-3

You can run the application by running the command:
```
python .\aioHttp3
```

Aditional libraries that might need to be installed:
```
pip install aiohttp
```

To set up the configuration, modify the SIMULATION_CONFIG parameters. Available parameters to be cofigured:
  * seed
  * targetDispatches
  * maxActiveCalls

Link to the github for source control:
```
https://github.com/AlinTcaci/hackathon_distancify
```

The UI module was degidned in Figma. In order to check the flow of the application, open Figma and in the Prototype section select Present.
Login page: allows the user to log in into his accout or create a new accout
Sign in page: allows the user to create an account using an email, name and a password
After Loggin In, the user can see the emergencies in the left side of the screen and a map with the current emergencies displayed on the right.
Using the search bar, the user can search for specific locations.
The "order by" is used to order the current emergencies after Date, County, City and Type (what type is the emergency. It needs Police, Medics or Firefighters). The type sorting consists of a sum of the number of emergencies of each type displayed by descending.
After selecting a city, the user can see on the map the number of alerts in that city and then select which one to dispatch first.
After selecting what problem to solve first, all specific cars are shown in the neighbouring towns. Then the user selects the city and the number of cars and dispatches them to the location.
After that, the emergency are deducted from the cars sent and a notification pops up notifying the user of the exact number of cars sent.
