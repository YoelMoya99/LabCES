from src.cls.dataAquisitionSystem import dataAquisitionSystem

def main() -> None:

    o = dataAquisitionSystem()
    o.presentState = o.state1

    while True:
        o.presentState()


if __name__ == "__main__":
    	
    main()
