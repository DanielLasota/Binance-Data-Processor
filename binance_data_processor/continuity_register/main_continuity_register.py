from binance_data_processor.continuity_register.continuity_register import ContinuityRegister


if __name__ == '__main__':
    continuity_register = ContinuityRegister()

    continuity_register.load_from_csv()

    for _ in continuity_register.continuity_entry_list:
        if _.instance_numer == 2:
            print(_)

    continuity_register.plot_timeline()
