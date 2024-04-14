import pandas as pd  # type: ignore

class Archiver():
    def __init__(self,election,year,departement,sections_type):
        # Not all combinations work : présidentielles 2017 22 cantons does
        #                             législatives 2016 22 cantons doesn't
        self.election = election # election type : présidentielles, législatives, européennes, régionales, départementales
        self.year = year # year, for now : 2017, 2019, 2021, 2022
        self.departement = departement # departement, for now : 22, 35
        self.sections_type = sections_type # type of section : circonscriptions, cantons
        
        # Make sure to be coherent in your choice, don't ask for circonscriptions and départementales
        #                                                 or for cantons and législatives
        
        
        self.data_file_name = f"Data/{election}/{election[:3]+str(year)}"+"_"+str(departement)+".ods"
        self.sections_file_name = f"Sections/{sections_type.capitalize()}/{year}/{sections_type}_{departement}_{year}.txt"
        
        # How are arranged the candidate informations for each election type
        self.candidate_informations = {"présidentielles":["S","N","F"],"législatives":["S","N","F","E"],"européennes":["E","U","WN"],"départementales":["WN","E"],"régionales":["E","E","U","WN"]}
        
        # Load the data file
        self.get_whole_data() 

    def init_model(self):
        # Get the indices of the useful data
        self.INSCRITS_index = self.columns.index("Inscrits")
        self.VOTANTS_index = self.columns.index("Votants")
        self.ABSTENTIONS_index = self.columns.index("Abstentions")
        self.BLANCS_index = self.columns.index("Blancs")
        self.NULS_index = self.columns.index("Nuls")
        self.EXPRIMES_index = self.columns.index("Exprimés")
        self.COMMUNE_index = self.columns.index("Libellé de la commune")

        # Define the data wanted to export
        self.model = {"Inscrits":0,
                      "Votants":0,
                      "Abstentions":0,
                      "Exprimés":0,
                      "B/N":0}
        
    def get_whole_data(self):
        # Read the ods file
        self.data = pd.read_excel(self.data_file_name, engine="odf")
        # Remove the blank lines or cells
        self.data = self.data.dropna(how="all")
        # Get the columns of the data set
        self.columns = self.data.columns.values.tolist()
        
    def get_candidates(self):
        # Get a row in the expected rows, for example get the first place's row -> we just want to determine the candidates
        common_row = self.data[self.data["Libellé de la commune"] == self.section_cities[0]].values.tolist()[0]

        # Create the candidates and raw_candidates lists for later
        candidates = []
        raw_candidates = []
        
        # Get the length of the raw candidate, the amount of informations per candidate
        name_length = len(self.candidate_informations[self.election])
        
        # Loop over each cell of the row
        for i in range(len(common_row)-name_length+1):
            # Get if the cell could be the starting point of a candidate name
            valid,name,raw_name = self.discriminate_name(common_row[i:i+name_length])
            # If it is, keep the informations
            if valid: 
                candidates.append(" ".join(name))
                raw_candidates.append(raw_name)

        # Return the displayed candidate name, and the used one to refer to them in the program
        return candidates,raw_candidates

    def discriminate_name(self,name):
        # Get the model of a name, in function the election type
        model = self.candidate_informations[self.election]
        
        # List of name's part kept to be displayed
        kept_name = []
        
        # If the value of valid is False, it means the name isn't one, so it'll return False
        valid = True
        for i,name_part in enumerate(name):
            # If the cell isn't a string, it can't be candidate's name or information
            if type(name_part) != str:
                valid = False
                break
            
            # Verification of Sex, should be one letter long
            if model[i] == "S":
                valid = len(name_part)==1
            # Verification of First Name, Whole Name, or Other, shouldn't be one letter long 
            if model[i] == "F" or model[i] =="WN" or model[i] == "U":
                valid = len(name_part)>1
                if model[i] != "U": kept_name.append(name_part) # Just keep the names
            # Verification of Name, should be already capitalized
            if model[i] == "N":
                valid = name_part == name_part.upper()
                kept_name.append(name_part)
            # Verification of other data, that should be capitalized
            if model[i] == "E" :
                valid = name_part.replace("-","") == name_part.replace("-","").upper()
                kept_name.append(name_part)
        
            # Valid verification, break in case it's false
            if not valid:
                break 
            
        return valid, kept_name, name

    def prepare_city_data(self,circonscription_identifiant):
        """
        Transforms the data set to a dictionary of city,
        Each city has a raw data : every vote place within the city
        And a data which will be completed later : votes for each candidate, vote place are summmed up
        """
        
        # For each city, get its data in a list object
        for city in self.section_cities: 
            # For each election different from législatives, only get corresponding city name
            if self.election != "législatives":
                self.section_dict[city] = {"raw data":self.data[self.data["Libellé de la commune"] == city].values.tolist(),
                                        "data":{**self.model,**self.candidates_results}}
                continue
            
            # For législatives make sure the circonscription is the right one
            self.section_dict[city] = {"raw data":self.data[(self.data['Code de la circonscription'] == circonscription_identifiant) & (self.data['Libellé de la commune'] == city)].values.tolist(),
                                      "data":{**self.model,**self.candidates_results}}
        
        # Also create the Total row
        self.section_dict["Total"] = {"raw data":None,
                                     "data":{**self.model,**self.candidates_results}}

    def get_city_general_data(self):
        # For each city, get the abstentions, inscrits etc
        for city in self.section_cities:
            # Make sure to sum each vote place
            for vote_place in range(len(self.section_dict[city]["raw data"])):
                self.section_dict[city]["data"]["Inscrits"] += self.section_dict[city]["raw data"][vote_place][self.INSCRITS_index]
                self.section_dict[city]["data"]["Votants"] += self.section_dict[city]["raw data"][vote_place][self.VOTANTS_index]
                self.section_dict[city]["data"]["Abstentions"] += self.section_dict[city]["raw data"][vote_place][self.ABSTENTIONS_index]
                self.section_dict[city]["data"]["Exprimés"] += self.section_dict[city]["raw data"][vote_place][self.EXPRIMES_index]
                # Blanc and Nuls are initially separated
                self.section_dict[city]["data"]["B/N"] += self.section_dict[city]["raw data"][vote_place][self.BLANCS_index]
                self.section_dict[city]["data"]["B/N"] += self.section_dict[city]["raw data"][vote_place][self.NULS_index]

    def get_city_candidates_data(self):
        # Get for each city, and for each candidate its votes
        for city in self.section_cities:
            for vote_place in range(len(self.section_dict[city]["raw data"])):
                # Looping over each city, and vote place, and now candidates
                for candidate in self.candidates:

                    is_valid = False
                    # Size of a raw name
                    name_size = len(self.candidate_informations[self.election])
                    
                    # Index of the candidate in the candidates list
                    candidate_index  = self.candidates.index(candidate)
                    # First information of the looked-for candidate, used to pre-discriminate cases
                    raw_candidate_discriminant = self.raw_candidates[candidate_index][0] # discriminant
                    # Pre-discriminations : only get indices where it could be  the candidate (Same name / Same Sex ...)
                    candidate_indices = [i for i, discriminant in enumerate(self.section_dict[city]["raw data"][vote_place]) if discriminant == raw_candidate_discriminant]
                    
                    # Testing the cell
                    for test_index in candidate_indices:
                        # If the cells following the main cell is exactly the raw candidate informations, it's they
                        is_valid = self.section_dict[city]["raw data"][vote_place][test_index:test_index+name_size] == self.raw_candidates[candidate_index]
                        if is_valid:
                            # Get the vote 
                            vote_offset = test_index+name_size
                            
                    self.section_dict[city]["data"][candidate] += self.section_dict[city]["raw data"][vote_place][vote_offset]

    def get_totals_data(self):
        # Just sum all the votes 
        for city in self.section_cities:
            for key,val in self.section_dict[city]["data"].items():
                self.section_dict["Total"]["data"][key] += val

    def get_all_data(self):
        # Get all the data
        self.get_city_general_data()
        self.get_city_candidates_data()
        self.get_totals_data()

    def finish_data_processing(self):
        self.final_data = {}
        
        # For each city, add the percentages etc
        for city in self.section_cities+["Total"]:
            # Create a new entry to the dictionary
            self.final_data[city] = []
            
            # Get the number of inscrits or exprimés
            ref_inscrits = self.section_dict[city]["data"]["Inscrits"]
            ref_exprimés = self.section_dict[city]["data"]["Exprimés"]
            
            #= Add all the informations expected =#

            # Inscrits
            self.final_data[city].append(ref_inscrits)
            
            # Votants and Votants-%I
            self.final_data[city].append(self.section_dict[city]["data"]["Votants"])
            self.final_data[city].append(round(100*self.section_dict[city]["data"]["Votants"]/ref_inscrits,3))
            # Abstentions and Abstentions-%I
            self.final_data[city].append(self.section_dict[city]["data"]["Abstentions"])
            self.final_data[city].append(round(100*self.section_dict[city]["data"]["Abstentions"]/ref_inscrits,3))
            # Exprimés and Exprimés-%I and Exprimés-%V
            self.final_data[city].append(ref_exprimés)
            self.final_data[city].append(round(100*ref_exprimés/ref_inscrits,3))
            self.final_data[city].append(round(100*ref_exprimés/self.section_dict[city]["data"]["Votants"],3))
            # Blancs/Nuls and Blancs/Nuls-%I and Blancs/Nuls-%V
            self.final_data[city].append(self.section_dict[city]["data"]["B/N"])
            self.final_data[city].append(round(100*self.section_dict[city]["data"]["B/N"]/self.section_dict[city]["data"]["Votants"],3))
            
            # For each candidate get the same informations
            # Votes and Votes-%I and Votes-%E
            for candidate in self.candidates:
                # Number of votes for this candidate
                votes = self.section_dict[city]["data"][candidate]
                self.final_data[city] += [votes,
                                          round(100*votes/ref_inscrits,3),
                                          round(100*votes/ref_exprimés,3)]
    
        self.formated_final_data = [[city]+self.final_data[city] for city in self.section_cities+["Total"]]

    def export_as_xlsx(self):
        # Create an header and a subheader in function of the candidates
        export_model_header = ["Communes","Inscrits","Votants","","Abstentions","","Exprimés","","","B/N",""]+[head for candidate in self.candidates for head in [candidate,"",""]]
        export_model_subheader = ["","","Voix","%","Voix","%","Voix","%I","%V","Voix","%V"]+["Voix","%I","%E"]*len(self.candidates)

        # Concatenate all the cities rows and the total one
        self.data_set_lines = [export_model_header,export_model_subheader]+self.formated_final_data
        
        # Create a new dataset from the lines
        exporting_data_set = pd.DataFrame(self.data_set_lines[1:], columns=self.data_set_lines[0])

        # Export it with ExcelWriter
        writer = pd.ExcelWriter(self.export_file_name, engine='xlsxwriter')
        exporting_data_set.to_excel(writer, index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Specify the formats

        # Header format for columns
        head_format = workbook.add_format()
        head_format.set_bold()
        head_format.set_align("center")
        head_format.set_border(1)
        head_format.set_valign("vcenter")        

        # Subheader format for Voix, %I, %E
        subhead_format = workbook.add_format()
        subhead_format.set_italic()
        subhead_format.set_align("center")
        subhead_format.set_border(1)

        # Data format for the main part of cells
        data_format = workbook.add_format()
        data_format.set_align("center")
        data_format.set_border(1)   

        # Total format for the total row, at the very end
        total_format = workbook.add_format()
        total_format.set_font_color("white")
        total_format.set_bg_color("black") 
        total_format.set_align("center")
        total_format.set_border(1)    
        total_format.set_bold()
        

        # Apply the formats and paste the data
        
        # Merge the cells on the first row
        for i in range(len(self.candidates)):
            worksheet.merge_range(f"{self.index_to_alpha(11+i*3)}1:{self.index_to_alpha(13+i*3)}1", self.candidates[i], head_format)
        
        worksheet.merge_range("A1:A2", self.data_set_lines[0][0], head_format)
        worksheet.merge_range("B1:B2", self.data_set_lines[0][1], head_format)
        worksheet.merge_range("C1:D1", self.data_set_lines[0][2], head_format)
        worksheet.merge_range("E1:F1", self.data_set_lines[0][4], head_format)
        worksheet.merge_range("G1:I1", self.data_set_lines[0][6], head_format)
        worksheet.merge_range("J1:K1", self.data_set_lines[0][9], head_format)

        # Write Voix, %I, %E on the second row
        worksheet.write_row('A2', self.data_set_lines[1], subhead_format)
        
        # Write the main data
        for i in range(len(self.section_cities)):
            worksheet.write_row(f'A{3+i}', self.data_set_lines[2+i], data_format)

        # Write Totals at the very end
        worksheet.write_row(f'A{3+len(self.section_cities)}', self.data_set_lines[-1], total_format)
        
        writer._save()

    def get_place_sections(self):
        with open(self.sections_file_name,"r",encoding="utf-8") as place_file:
            sections = [[city for city in sections.split("\n") if city] for sections in "".join(place_file.readlines()).split("--\n")]
        return sections

    def archive_place(self,section,export_file_name,circonscription_identifiant=0):
        # Recreate the base to store my section datas
        self.section_dict = {}
        self.export_file_name = export_file_name
        
        # Initialise all the data that will be useful later
        self.init_model()

        # Get the cities in the section
        self.section_cities = section
        # Get the candidates and the raw candidates
        self.candidates,self.raw_candidates = self.get_candidates()
        # Create a blank dictionary, as is self.model
        self.candidates_results = {candidate:0 for candidate in self.candidates}        

        # Run the data collecting
        self.prepare_city_data(circonscription_identifiant) # Create the dicts etc, get the vote places, cities
        self.get_all_data() # Get the votes amount per candidate per city
        self.finish_data_processing() # Calculate the extra data
        # Export the data
        self.export_as_xlsx()

    def archive_all(self):
        self.sections = self.get_place_sections()

        for i in range(len(self.sections)):
            if self.sections_type == "circonscriptions":
                export_name = "Exported Data/Newly created/"+str(i+1)+(not i)*"r"+f"e_circonscription_du_{self.departement}_{self.election[0]}{self.year}.xlsx",
            else:
                export_name = "Exported Data/Newly created/"+f"canton_de_{self.sections[i][0]}_{self.election[0]}{self.year}.xlsx"
            
            self.archive_place(self.sections[i],export_name,circonscription_identifiant=i+1)

    def index_to_alpha(self,index):
        full_alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        digits = []
        q = index // 26
        r = index % 26
        digits.append(r)
        
        while q > 0:
            r = q % 26
            q = q // 26
            digits.append(r-1)

        return "".join(reversed([full_alphabets[rest] for rest in digits]))

# Get the results of the {élections départementales de 2021 dans le 22 : les communes étant regroupées par cantons}
instance = Archiver("départementales",2021,22,"cantons")
instance.archive_all()