import asyncio
from src.cls.modbusClass import modbusComunication


class orquestrator:
    def __init__(self):
        self.oCom = modbusComunication()


    async def systemConfiguration(self):
    
        print('This is the code for the sys config')
        await self.oCom.modbusConfiguration()
        await self.oCom.modbusConnect()


    async def mainLoop(self):

        print('This is the main loop working')
        while True:
            temp = await self.oCom.modbusDataRequest()
            print(f'this are the regs {temp}')    
            await asyncio.sleep(5)

    async def main(self):
        await self.systemConfiguration()
        await self.mainLoop()

if __name__ == '__main__':

    obj = orquestrator()
    asyncio.run(obj.main())


